import json
import requests
import pandas as pd
import os
import time

from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse

def build_url(path, query=""):
    # type: (str, str) -> str
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", "business-api.tiktok.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

def get_credentials() -> dict:

    credentials_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tokens', 'tiktok_credentials.json'))
    with open(credentials_path, 'r') as f:
        credentials = json.load(f)
    
    return credentials

def get_account_id(ad_account: list) -> str:

    url_path = "/open_api/oauth2/advertiser/get/"

    credentials = get_credentials()
    access_token = credentials['access_token']
    secret = credentials['secret']
    app_id = credentials['app_id']

    query = '''{
        \"access_token\": \"%s\", 
        \"secret\": \"%s\", 
        \"app_id\": \"%s\"
        }''' % (access_token, secret, app_id)

    args = json.loads(query)
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
    url = build_url(url_path, query_string)

    response = requests.get(url).json()

    accounts_df = pd.DataFrame(response['data']['list'])

    account_id = str(accounts_df.loc[accounts_df['advertiser_name'].str.lower().isin([x.lower() for x in ad_account]), 'advertiser_id'].unique()[0])

    return account_id

def get_sync(url: str, headers:dict) -> pd.DataFrame:

    response = requests.get(url, headers = headers).json()

    data_dict = {
        'date': [],
        'campaign_name': [],
        'campaign_id': [],
        'adset_name': [],
        'adset_id': [],
        'ad_name': [],
        'ad_id': [],
        'objective': [],
        'post_text': [],
        'cost': [],
        'impressions': [],
        'clicks': [],
        'video_views': [],
        'w25_views': [],
        'w50_views': [],
        'w75_views': [],
        'w100_views': []
    }

    for result in response['data']['list']:
        data_dict['date'].append(result['dimensions']['stat_time_day'])
        data_dict['ad_id'].append(result['dimensions']['ad_id'])
        data_dict['campaign_name'].append(result['metrics']['campaign_name'])
        data_dict['campaign_id'].append(result['metrics']['campaign_id'])
        data_dict['adset_name'].append(result['metrics']['adgroup_name'])
        data_dict['adset_id'].append(result['metrics']['adgroup_id'])
        data_dict['ad_name'].append(result['metrics']['ad_name'])
        data_dict['objective'].append(result['metrics']['objective_type'])
        data_dict['post_text'].append(result['metrics']['ad_text'])
        data_dict['cost'].append(result['metrics']['spend'])
        data_dict['impressions'].append(result['metrics']['impressions'])
        data_dict['clicks'].append(result['metrics']['clicks'])
        data_dict['video_views'].append(result['metrics']['video_play_actions'])
        data_dict['w25_views'].append(result['metrics']['video_views_p25'])
        data_dict['w50_views'].append(result['metrics']['video_views_p50'])
        data_dict['w75_views'].append(result['metrics']['video_views_p75'])
        data_dict['w100_views'].append(result['metrics']['video_views_p100'])

    df = pd.DataFrame(data_dict)
    float_numbers = ['cost']
    int_numbers = ['impressions', 'clicks', 'video_views', 'w25_views', 'w50_views', 'w75_views', 'w100_views']
    non_numbers = [column for column in df.columns if column not in (float_numbers + int_numbers + ['date'])]

    df['source'] = 'tiktok'
    
    df[float_numbers] = df[float_numbers].astype(float)
    df[int_numbers] = df[int_numbers].astype(int)
    df[non_numbers] = df[non_numbers].astype(str)
    df['date'] = pd.to_datetime(df['date'])

    df.sort_values('date', ignore_index = True, inplace = True)

    return df

def get_async(advertiser_id: str, url: str, headers: dict) -> pd.DataFrame:

    response = requests.post(url, headers = headers).json()

    task_id = response['data']['task_id']

    check_path = "/open_api/v1.2/reports/task/check/"
    download_path = "/open_api/v1.2/reports/task/download/"

    query = '''{
        \"task_id\": \"%s\",
        \"advertiser_id\": \"%s\"
        }''' % (task_id, advertiser_id)
    
    args = json.loads(query)
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
    check_url = build_url(check_path, query_string)

    check_report = requests.get(check_url, headers = headers).json()
    
    while check_report['data']['status'] != 'SUCCESS':

        print(check_report['data']['status'])
        time.sleep(1)
        check_report = requests.get(check_url, headers = headers).json()

        if check_report['data']['status'] == 'FAILED':
            print(check_report['data']['message'])
            return pd.DataFrame([])

    download_url = build_url(download_path, query_string)

    report = requests.get(download_url, headers = headers).content

    with open('tiktok_temp.csv', 'w', newline='\n', encoding='iso-8859-1') as f:
        f.write(report.decode('iso-8859-1'))

    df = pd.read_csv('tiktok_temp.csv')

    os.remove('tiktok_temp.csv')

    df.fillna('', inplace = True)
    df.rename(columns = {column: column.lower().replace(' ', '_') if 'group' not in column.lower() else column.lower().replace(' ', '').replace('group', 'set_') for column in df.columns}, inplace = True)
    df.rename(columns = {
        'impression': 'impressions',
        'click': 'clicks',
        'text': 'post_text',
        'video_views_at_25%': 'w25_views',
        'video_views_at_50%': 'w50_views',
        'video_views_at_75%': 'w75_views',
        'video_views_at_100%': 'w100_views'
        }, inplace = True)
    
    float_numbers = ['cost']
    int_numbers = ['impressions', 'clicks', 'video_views', 'w25_views', 'w50_views', 'w75_views', 'w100_views']
    non_numbers = [column for column in df.columns if column not in (float_numbers + int_numbers + ['date'])]

    df['source'] = 'tiktok'

    df[float_numbers] = df[float_numbers].astype(float)
    df[int_numbers] = df[int_numbers].astype(int)
    df[non_numbers] = df[non_numbers].astype(str)
    df['date'] = pd.to_datetime(df['date'])

    df.sort_values('date', ignore_index = True, inplace = True)

    return df

def get_response(account_id: str, start_date: str, end_date: str) -> pd.DataFrame:

    access_token = get_credentials()['access_token']

    url_path = "/open_api/v1.2/reports/integrated/get/"

    metrics_list = [
        'campaign_name', 
        'campaign_id', 
        'adgroup_name', 
        'adgroup_id', 
        'ad_name', 
        'ad_text', 
        'objective_type', 
        'spend', 
        'impressions',
        'clicks',
        'video_play_actions',
        'video_views_p25',
        'video_views_p50',
        'video_views_p75',
        'video_views_p100'
    ]

    dimensions_list = [
        'ad_id', 
        'stat_time_day'
    ]

    metrics = json.dumps(metrics_list)
    dimensions = json.dumps(dimensions_list)

    data_level = 'AUCTION_AD'
    lifetime = False
    report_type = 'BASIC'
    service_type = 'AUCTION'
    page = 1
    page_size = 200

    # Args in JSON format
    query = '''{
        \"metrics\": %s, 
        \"data_level\": \"%s\", 
        \"end_date\": \"%s\", 
        \"page_size\": \"%s\", 
        \"start_date\": \"%s\", 
        \"advertiser_id\": \"%s\", 
        \"service_type\": \"%s\", 
        \"lifetime\": \"%s\", 
        \"report_type\": \"%s\", 
        \"page\": \"%s\", 
        \"dimensions\": %s
        }''' % (metrics, data_level, end_date, page_size, start_date, account_id, service_type, lifetime, report_type, page, dimensions)
    
    args = json.loads(query)
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})

    url = build_url(url_path, query_string)
    headers = {
        'Access-Token': access_token
    }

    if (pd.to_datetime(end_date).date() - pd.to_datetime(start_date).date()).days < 30:
        return get_sync(url, headers)

    return get_async(account_id, url, headers)


def get_tiktok(account: str, start_date: str, end_date: str) -> pd.DataFrame:

    account_id = get_account_id(account)
    df = get_response(account_id, start_date, end_date)
    
    return df


if __name__ == '__main__':

    df = get_tiktok(['BB | LL | AN | Agro Crédito Safrinha'], '2022-06-08', '2022-07-10')
    print(df.head())
    # df.to_excel('tiktok_prep2.xlsx', sheet_name = 'tiktok_prep', index = False)