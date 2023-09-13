# from tracemalloc import start
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.ad import Ad


import pandas as pd

import os
import json
import requests
import time
import datetime

def get_credentials() -> dict:
    '''Carrega e atualiza as credenciais.'''

    credentials_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tokens', 'facebook_credentials.json'))
    with open(credentials_path, 'r') as f:
        credentials = json.load(f)

    app_id = credentials['app_id']
    app_secret = credentials['app_secret']
    access_token = credentials['access_token']

    params = {
        'grant_type': 'fb_exchange_token',
        'client_id': app_id,
        'client_secret': app_secret,
        'fb_exchange_token': access_token
    }

    url = f'https://graph.facebook.com/v13.0/oauth/access_token'
    new_access_token = requests.get(url, params = params).json()['access_token']
    credentials['access_token'] = new_access_token

    with open(credentials_path, 'w') as f:
        json.dump(credentials, f)

    return credentials

def facebook_init(credentials: dict) -> None:
    '''Inicia a sessão com a API do faceobok.'''

    FacebookAdsApi.init(credentials['app_id'], credentials['app_secret'], credentials['access_token'])

    return

def get_accounts_ids(ad_accounts: list, credentials) -> list:
    '''Pega os ids das contas que serão utilizadas para montagem da base.'''
    
    access_token = credentials['access_token']
    response = requests.get(f'https://graph.facebook.com/v13.0/me/adaccounts?fields=name&limit=60&access_token={access_token}').json()

    all_accounts = response['data']
    all_accounts = pd.DataFrame(all_accounts)

    accounts_id = list(all_accounts.loc[all_accounts['name'].str.lower().isin([x.lower() for x in ad_accounts]), 'id'].unique())

    return accounts_id

def init_account(account_id: str) -> AdAccount:
    '''Inicia a conta na API.'''

    return AdAccount(account_id)

def get_analytics(account: AdAccount, init_date: str, end_date: str) -> pd.DataFrame:
    '''
    Faz o requerimento do relatório de insights para a API do Facebook.
    - entradas: 
        - objeto da conta do Facebook Ads
        - data inical dos dados a serem coletados
        - data final dos dados a serem coletados

    - saídas:
        - dataframe do dados de métricas nos dias selecionados
    '''
    
    fields = [
        'created_time',
        'account_name',
        'account_id',
        'campaign_name',
        'campaign_id',
        'adset_name',
        'adset_id',
        'ad_name',
        'ad_id',
        'objective',
        'impressions', 
        'spend',
        'clicks',
        'video_p25_watched_actions',
        'video_p50_watched_actions',
        'video_p75_watched_actions',
        'video_p100_watched_actions',
        'inline_post_engagement',
        'dda_results',
        'actions'
        ]

    params = {
        'level': 'ad',
        'fields': fields,
        'time_increment': '1',
        'time_range': {'since': init_date, 'until': end_date},
        'breakdowns': ['publisher_platform'],
        'sort': ['date_start'],
        'action_breakdown': ['action_type']
    }

    i_async_job = account.get_insights(params = params, is_async = True)

    while True:
        job = i_async_job.api_get()
        time.sleep(40) # Colocar condição para mudar o tempo dependendo da quantidade de dias para pegar os dados
        if job:
            break
    
    df = pd.DataFrame(i_async_job.get_result())

    if df.empty:
        return df

    df.drop(df[df['publisher_platform'] == 'unknown'].index, inplace = True)
    df.reset_index(drop = True, inplace = True)

    return df

def fix_actions(df: pd.DataFrame) -> pd.DataFrame:
    '''Transforma a coluna actions do dataframe nas colunas respectivas de cada ação.'''

    video_columns = [
        'video_p25_watched_actions',
        'video_p50_watched_actions',
        'video_p75_watched_actions',
        'video_p100_watched_actions'
        ]

    video_name = [
        'w25_views',
        'w50_views',
        'w75_views',
        'w100_views'
    ]

    for column in video_columns:
        if column not in df.columns:
            df[column] = 0
        else:
            df.loc[~df[column].isna(), column] = df.loc[~df[column].isna(), column].str[0].str['value']

    df.fillna(0, inplace = True)
    df.rename(columns = dict(zip(video_columns, video_name)), inplace = True)

    actions_dict = {}
    actions_list = []

    for _, row in df.iterrows():
        if isinstance(row['actions'], list):
            for action in row['actions']:
                actions_dict[action['action_type']] = action['value']
            actions_list.append(actions_dict)
            actions_dict = {}
        else:
            actions_list.append({'no_value': ''})

    actions_df = pd.DataFrame(actions_list).fillna(0)

    fixed_df = pd.concat([df, actions_df], axis = 1)

    return fixed_df


def get_creatives(df: pd.DataFrame, account: AdAccount) -> pd.DataFrame:

    

    ads_dict = {
        'ad_id': [],
        'creative_id': []
    }

    ads = account.get_ads(fields = [Ad.Field.creative])

    for ad in ads:
        ads_dict['ad_id'].append(ad['id'])
        ads_dict['creative_id'].append(ad['creative']['id'])

    ads_df = pd.DataFrame(ads_dict)

    df = df.merge(ads_df, on = 'ad_id', how = 'left')

    df.drop(df[df.creative_id.isna()].index, inplace = True)
    df.reset_index(inplace = True)
    
    creative_ids = list(df.creative_id.unique())
    creative_fields = [
        'object_story_spec',
        'url_tags',
        'effective_object_story_id',
        'object_type',
        'thumbnail_url',
        'body',
        'image_url',
        'instagram_permalink_url',
        'object_url'
    ]

    creatives = [dict(AdCreative(creative).api_get(fields = creative_fields)) for creative in creative_ids]

    creative_dict = {
        'creative_id': [],
        'post_id': [],
        'destination_url': [],
        'media_url': [],
        'url_tags': [],
        'post_text': [],
        'message': [],
        'object_type': [],
        'thumbnail_url': [],
        'instagram_url': []
    }

    for creative in creatives:
        creative_dict['creative_id'].append(creative['id'])
        creative_dict['post_id'].append(creative['effective_object_story_id'])
        creative_dict['object_type'].append(creative['object_type'])
        creative_dict['thumbnail_url'].append(creative['thumbnail_url']) if 'thumbnail_url' in creative.keys() else creative_dict['thumbnail_url'].append('')

        creative_dict['instagram_url'].append(creative['instagram_permalink_url']) if 'instagram_permalink_url' in creative.keys() else creative_dict['instagram_url'].append('')
        creative_dict['media_url'].append(creative['image_url']) if 'image_url' in creative.keys() else creative_dict['media_url'].append('')
        creative_dict['url_tags'].append(creative['url_tags']) if 'url_tags' in creative.keys() else creative_dict['url_tags'].append('')
        creative_dict['post_text'].append(creative['body']) if 'body' in creative.keys() else creative_dict['post_text'].append('')

        if 'object_story_spec' in creative.keys():
            if 'link_data' in creative['object_story_spec'].keys():
                # print(creative['object_story_spec']['link_data']['link'])
                creative_dict['destination_url'].append(creative['object_story_spec']['link_data']['link']) if 'link' in creative['object_story_spec']['link_data'].keys() else creative_dict['destination_url'].append('')
                creative_dict['message'].append(creative['object_story_spec']['link_data']['message']) if 'message' in creative['object_story_spec']['link_data'].keys() else creative_dict['message'].append('')
            
            elif 'video_data' in creative['object_story_spec'].keys():
                # print(creative['object_story_spec']['video_data']['link'])
                creative_dict['destination_url'].append(creative['object_story_spec']['video_data']['link']) if 'link' in creative['object_story_spec']['video_data'].keys() else creative_dict['destination_url'].append('')
                creative_dict['message'].append(creative['object_story_spec']['video_data']['message']) if 'message' in creative['object_story_spec']['video_data'].keys() else creative_dict['message'].append('')
            
            else:
                creative_dict['destination_url'].append('')
                creative_dict['message'].append('')
                
        else:
            creative_dict['destination_url'].append('')
            creative_dict['message'].append('')

    creative_df = pd.DataFrame(creative_dict)

    df = df.merge(creative_df, on = 'creative_id', how = 'left')

    return df

def fix_types(df: pd.DataFrame) -> pd.DataFrame:

    df.fillna('', inplace = True)

    numeric_float = ['cost']   

    strings = [
        'account_id', 'account_name', 'ad_id', 'ad_name', 'adset_id',
        'adset_name', 'campaign_id', 'campaign_name', 'creative_id', 
        'post_id', 'destination_url', 'media_url', 'objective',
        'post_text', 'object_type', 'source', 'source_url',
        'medium', 'campaign', 'content'
        ] 

    date = ['date']

    numeric_int = [column for column in df.columns if column not in (numeric_float + strings + date)]

    df[numeric_int] = df[numeric_int].astype(int)
    df[numeric_float] = df[numeric_float].astype(float)
    df[strings] = df[strings].astype(str)

    df['date'] = pd.to_datetime(df['date'], format = '%Y-%m-%d')

    return df

def manipulate_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    df['source'] = ''
    df['source_url'] = ''
    df['medium'] = ''
    df['campaign'] = ''
    df['content'] = ''

    df['source'] = df['publisher_platform'].map(lambda x: 'fb' if x != 'instagram' else 'ig') 

    df.loc[df['post_text'] == '', 'post_text'] = df.loc[df['post_text'] == '', 'message']
    df.loc[df['media_url'] == '', 'media_url'] = df.loc[df['media_url'] == '', 'thumbnail_url']

    df.loc[df['source'] == 'fb', 'source_url'] = 'https://www.facebook.com/' +  \
        df['post_id'].str.split('_', expand = True)[0] + '/posts/' + \
            df['post_id'].str.split('_', expand = True)[1] + '/'

    df.loc[df['source'] == 'ig', 'source_url'] = df.loc[df['source'] == 'ig', 'instagram_url'] 

    no_content = df[~df['url_tags'].str.contains('content')]
    df.drop(df[~df['url_tags'].str.contains('content')].index, inplace = True)
    
    if df.shape[0] > 0:
        df['medium'] = df['url_tags'].str.replace('&', '').str.split('utm_', expand = True)[2].str.split('=', expand = True)[1]
        df['campaign'] = df['url_tags'].str.replace('&', '').str.split('utm_', expand = True)[3].str.split('=', expand = True)[1]
        df['content'] = df['url_tags'].str.replace('&', '').str.split('utm_', expand = True)[5].str.split('=', expand = True)[1]

    df = pd.concat([df, no_content], axis = 0, ignore_index = True)

    columns_to_drop = [
        'publisher_platform', 
        'message', 
        'thumbnail_url', 
        'clicks', 
        'inline_post_engagement',  
        'instagram_url',
        'url_tags',
        'actions',
        'date_stop', 
        'created_time',
        'page_engagement'
        ]

    if 'no_value' in df.columns:
        columns_to_drop.append('no_value')

    if 'post' in df.columns:
        columns_to_drop.append('post')
        
    df.drop(columns = columns_to_drop, inplace = True)
    
    df.rename(columns = {column: column.split('.')[1] for column in df.columns if 'onsite' in column}, inplace = True)
    df.rename(columns = {column: column.split('.')[1] for column in df.columns if 'app_custom_event' in column}, inplace = True)
    df.rename(columns = {'spend': 'cost', 'date_start': 'date', 'link_click': 'clicks', 'video_view': 'video_views'}, inplace = True)

    if 'view_content' in df.columns:
        df.rename(columns = {'view_content': 'results'}, inplace = True)

    df = fix_types(df)

    return df

def get_facebook(accounts: list, start_date: str, end_date: str) -> pd.DataFrame:
    
    credentials = get_credentials()

    facebook_init(credentials)

    accounts_ids = get_accounts_ids(accounts, credentials)

    df_list = []

    for id in accounts_ids:

        account = init_account(id)
        analytics_df = get_analytics(account, start_date, end_date)

        if analytics_df.empty:
            print('Não há novos dados em Facebook.')
            df_list.append(analytics_df)

        else:
            fixed_df = fix_actions(analytics_df)
            df_creatives = get_creatives(fixed_df, account)
            clean_df = manipulate_dataframe(df_creatives)
            df_list.append(clean_df)    

    facebook_df = pd.concat(df_list)

    return facebook_df

#def main():
#    print(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tokens', 'facebook_credentials.json')))

#if __name__ == '__main__':

#    main()

#    get_facebook_data(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..')), ['BB | LL | APOIO MC | ESTILO'], '2022-02-10', '2022-02-11')