from fileinput import filename
import os
import time
import random
import io
import json

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient import http
from datetime import date
from datetime import timedelta

import pandas as pd

# LINK DOCUMENTAÇÃO
# https://github.com/googleads/googleads-dfa-reporting-samples/tree/master/python/v3_5

# As variáveis a seguir controlam o comportamento de tentativas enqaunto o relatório está sendo processado.
# Mínimo de tempo entre requerimentos. Padrão de 10 segundos.
MIN_RETRY_INTERVAL = 10
# Máximo tempo entre requerimentos. Padrão de 1 minuto.
MAX_RETRY_INTERVAL = 1 * 60
# Máximo de tempo passando fazendo requerimentos. Padrão de 5 minutos.
MAX_RETRY_ELAPSED_TIME = 5 * 60

# Tamanho do chunk quando tiver fazendo download do relatório. Padrão 32MB.
CHUNK_SIZE = 32 * 1024 * 1024

def get_google_credentials() -> Credentials:
    '''Confirma / atualiza as credenciais de OAuth 2.0 para utilização das APIs.'''
   
    secrets_path =  os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tokens', 'google_secret.json'))
    credentials_path =  os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tokens', 'google_credentials.json'))

    scopes = ['https://www.googleapis.com/auth/dfareporting', 
            'https://www.googleapis.com/auth/ddmconversions',
            'https://www.googleapis.com/auth/dfatrafficking']

    credentials = None

    if os.path.exists(credentials_path):
        credentials = Credentials.from_authorized_user_file(credentials_path, scopes)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(secrets_path, scopes)
            credentials = flow.run_local_server()
        with open(credentials_path, 'w') as token:
                token.write(credentials.to_json())

    return credentials

def start_service(credentials: Credentials):
    '''Começa o serviço para extração de dados da API.'''
    # Ficar atento a versão da API pois ela pode mudar.
    # Versão mais recente em: https://developers.google.com/doubleclick-advertisers/rel_notes

    api_version = 'v3.5'
    service = build('dfareporting', api_version, credentials = credentials)

    return service

def get_profileId(service) -> str:
    '''Extrai o ID de perfil da conta, necessário para quase todas as operações.'''

    userProfile = service.userProfiles().list().execute()
    profile_id = userProfile['items'][0]['profileId']

    return profile_id

def define_report(report_name: str, start_date: str, end_date: str) -> dict:
    '''
    Define a estrutura do relatório (dimensões/métricas).
    O formato do relatório é de um json com alguns campos a serem preenchidos:
    - name: nome do relatório no CM.
    - type: tipo de relatório do CM.
    - fileName: nome do arquivo caso baixado direto do CM. (Opcional)
    - format: formato do arquivo, pode ser CSV ou Excel. (Opcional)
    - criteria: neste argumento se define o relatório.

    Criteria recebe os seguintes argumentos:
    - dateRange: dicionário que contém a faixa de dias que serão puxados no relatório.
    - dimensions: lista de dicionários com os nomes das dimensões que serão puxadas.
    - metricNames: lista de métricas que se deseja puxar.
    '''

    report = {
        'name': report_name,
        'type': 'STANDARD',
        'format': 'CSV'
    }

    criteria = {
        'dateRange': {
            'startDate': start_date,
            'endDate': end_date
        }
    }
    
    # As dimesões utilizadas nos relatórios diários (preps) são diferentes dos utilizados
    # em relatórios com o checking.

    # lista de dimensões/métricas: https://developers.google.com/doubleclick-advertisers/v3.5/dimensions

    criteria['dimensions'] = [
        {'name': 'campaign'},
        {'name': 'campaignId'},
        {'name': 'site'},
        {'name': 'placement'},
        {'name': 'placementId'},
        {'name': 'creative'},
        {'name': 'creativeId'},
        {'name': 'date'}
    ]
    
    criteria['metricNames'] = [
        'mediaCost',
        'clicks',
        'impressions',
        'activeViewMeasurableImpressions',
        'activeViewViewableImpressions',
        'richMediaVideoPlays',
        'richMediaVideoFirstQuartileCompletes',
        'richMediaVideoMidpoints',
        'richMediaVideoThirdQuartileCompletes',
        'richMediaVideoCompletions'
    ]

    report['criteria'] = criteria

    return report

def add_filters(service, report, profile_id, campaign_list):
    '''Adiciona filtros de dimensões para puxar apenas as campanhas/fontes necessárias.'''

    campaign_filter_request = {
        'dimensionName': 'campaign',
        'startDate': report['criteria']['dateRange']['startDate'],
        'endDate': report['criteria']['dateRange']['endDate']
    }

    source_filter_request = {
        'dimensionName': 'site',
        'endDate': report['criteria']['dateRange']['endDate'],
        'startDate': report['criteria']['dateRange']['startDate']
    }

    exclude_sources = [
        'Facebook Brasil', 'Google Display Network', 'Twitter - Official', 'Youtube BR', 
        'br.linkedin.com', 'TWITTER-OFFICIAL', 'Twitter', 'Youtube - Google Ads',
        'Google Ads: Display Remarketing', 'Google Ads', 'Instagram BR', 'Linkedin', 'Tik Tok BR'
    ]

    sources = service.dimensionValues().query(profileId = profile_id, body = source_filter_request).execute()
    campaigns = service.dimensionValues().query(profileId = profile_id, body = campaign_filter_request).execute()

    campaign_filter = [campaign for campaign in campaigns['items'] if campaign['value'] in campaign_list]
    source_filter = [source for source in sources['items'] if source['value'] not in exclude_sources]

    report['criteria']['dimensionFilters'] = campaign_filter + source_filter

    return report


def create_report(service, report, profile_id):
    '''Cria o relatório no CM.'''
    return service.reports().insert(profileId = profile_id, body = report).execute()

def next_sleep_interval(previous_sleep_interval):
  """Calcula o próximo intervalo de silêncio baseado no anterior."""

  min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
  max_interval = previous_sleep_interval * 3 or MIN_RETRY_INTERVAL
  return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))


def run_report(service, profile_id, report_id):
    '''Executa o relatório.'''
    
    report_file = service.reports().run(profileId = profile_id, reportId = report_id).execute()
    file_id = report_file['id']

    sleep = 0
    start_time = time.time()
    
    while True:
        
        report_file = service.files().get(reportId = report_id, fileId = file_id).execute()

        status = report_file['status']

        if status == 'REPORT_AVAILABLE':
            print(f'[{status}] Relatório pronto pra download!')
            return report_file
        elif status != 'PROCESSING':
            print(f'[{status}] Processo falhou!')
            return
        elif time.time() - start_time > MAX_RETRY_ELAPSED_TIME:
            print('Tempo de processamento ultrapassou o limite')
            return
        
        sleep = next_sleep_interval(sleep)
        print(f'[{status}] Dormindo por {sleep} segundos.')
        time.sleep(sleep)


def download_report(campaign_path, service, report_file):
    '''Baixa o relatório.'''

    report_id = report_file['reportId']
    file_id = report_file['id']

    file_path = os.path.abspath(os.path.join(campaign_path, 'data', 'cm_prep.csv'))

    out_file = io.FileIO(file_path, mode = 'wb')

    request = service.files().get_media(reportId = report_id, fileId = file_id)

    downloader = http.MediaIoBaseDownload(out_file, request, chunksize = CHUNK_SIZE)

    download_finished = False
    while not download_finished:
        _, download_finished = downloader.next_chunk()

    return

def make_df(campaign_path):
    blank = 0
    file_path = os.path.abspath(os.path.join(campaign_path, 'data', 'cm_prep.csv'))

    with open(file_path, 'r') as text:
        for num, line in enumerate(text, 1):
            if 'Report Fields' in line:
                break
            if line == '\n':
                blank += 1

    df = pd.read_csv(file_path, header = num - blank)

    rename_columns = {
        'Campaign': 'campaign',
        'Campaign ID': 'campaign_id',
        'Site (CM360)': 'source',
        'Placement': 'placement',
        'Placement ID': 'placement_id',
        'Creative': 'cm_creative',
        'Creative ID': 'cm_creative_id',
        'Date': 'date',
        'Media Cost': 'cost',
        'Clicks': 'clicks',
        'Impressions': 'impressions',
        'Active View: Measurable Impressions': 'measurable_impressions',
        'Active View: Viewable Impressions': 'viewable_impressions',
        'Video Plays': 'video_views',
        'Video First Quartile Completions' : 'w25_views',
        'Video Midpoints': 'w50_views',
        'Video Third Quartile Completions': 'w75_views',
        'Video Completions': 'w100_views'
    }

    df.rename(columns = rename_columns, inplace = True)
    df.drop(df.index.max(), inplace = True)

    df['date'] = pd.to_datetime(df['date'])
    df.sort_values('date', ignore_index = True, inplace = True)

    os.remove(file_path)
    return df

def get_CampaignManager(campaign_path: str, campaigns: list, start_date: str, end_date: str):
    
    credentials = get_google_credentials()
    service = start_service(credentials)
    profile_id = get_profileId(service)

    report = define_report('report', start_date, end_date)
    report = add_filters(service, report, profile_id, campaigns)

    created_report = create_report(service, report, profile_id)

    report_file = run_report(service, profile_id, created_report['id'])

    download_report(campaign_path, service, report_file)

    df = make_df(campaign_path)
    
    return df

#if __name__ == '__main__':
#    main()