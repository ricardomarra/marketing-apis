from pydoc import importfile
import requests

import pandas as pd
import numpy as np

import os
import json


def get_credentials() -> dict:

    credentials_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tokens', 'linkedin_credentials.json'))
    with open(credentials_path, 'r') as f:
        credentials = json.load(f)

    # refresh_token = credentials['refresh_token']
    # client_id = credentials['client_id']
    # client_secret = credentials['client_secret']

    # params = {
    #     'grant_type': 'refresh_token',
    #     'refresh_token': refresh_token,
    #     'client_id': client_id,
    #     'client_secret': client_secret
    # }

    # headers = {
    #     'Content-Type': 'application/x-www-form-urlencoded'
    # }

    # new_access_token = requests.post('https://www.linkedin.com/oauth/v2/accessToken', params = params, headers = headers).json()['access_token']
    # credentials['access_token'] = new_access_token

    # with open(credentials_path, 'w') as f:
    #     json.dump(credentials, f)

    return credentials


def get_account(credentials: dict, account_name: list) -> str:

    access_token = credentials['access_token']

    header = {
        'Authorization': f'Bearer {access_token}'
    }

    params = {
        'search.type.values[0]': 'ENTERPRISE',
        'search.status.values[0]': 'ACTIVE'
    }

    accounts_r = requests.get('https://api.linkedin.com/v2/adAccountsV2?q=search', headers = header, params = params).json()
    
    accounts = pd.DataFrame(accounts_r['elements'])
    accounts = accounts[['name', 'id']]
    account_id = list(accounts.loc[accounts['name'].str.lower().isin([x.lower() for x in account_name]), 'id'].unique())[0]
    
    return account_id

def get_analytics(credentials: dict, account_id: str, start_date: str, end_date: str) -> pd.DataFrame:

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    fields = [
        'dateRange',
        'impressions',
        'clicks',
        'likes',
        'shares',
        'totalEngagements',
        'costInLocalCurrency',
        'pivot',
        'pivotValue',
        'videoViews',
        'videoFirstQuartileCompletions',
        'videoMidpointCompletions',
        'videoThirdQuartileCompletions',
        'videoCompletions'
    ]


    params = {
        'q': 'analytics',
        'dateRange.start.day': start_date.day,
        'dateRange.start.month': start_date.month,
        'dateRange.start.year': start_date.year,
        'dateRange.end.day': end_date.day,
        'dateRange.end.month': end_date.month,
        'dateRange.end.year': end_date.year,
        'timeGranularity': 'DAILY',
        'pivot': 'CREATIVE',
        'accounts': f'urn:li:sponsoredAccount:{account_id}',
        'fields': ','.join(fields)
    }

    access_token = credentials['access_token']

    header = {
        'Authorization': f'Bearer {access_token}'
    }

    r_response = requests.get('https://api.linkedin.com/v2/adAnalyticsV2?', headers = header, params = params).json()

    for element in r_response['elements']:
        element_day = element['dateRange']['start']['day']
        element_month = element['dateRange']['start']['month']
        element_year = element['dateRange']['start']['year']
        element['date'] = pd.to_datetime(f'{element_year}-{element_month}-{element_day}')
        
    analytics = pd.DataFrame(r_response['elements'])
    return analytics

def get_info(credentials: dict, df: pd.DataFrame):

    access_token = credentials['access_token']
    header = {
        'Authorization': f'Bearer {access_token}'
    }

    creatives = list(df['pivotValue'].unique())
    creatives_id = [creative.split('e:')[1] for creative in creatives]

    creative_list = []
    for id in creatives_id:
        creative_list.append(requests.get(f'https://api.linkedin.com/v2/adCreativesV2/{id}', headers = header).json())

    creative_df = pd.DataFrame(creative_list)
    creative_df['creative_id'] = pd.Series(creatives)
    creative_df = creative_df[['reference', 'campaign', 'creative_id']] if 'reference' in creative_df.columns else creative_df[['campaign', 'creative_id']]

    creative_df.dropna(inplace = True)
    campaigns = list(creative_df['campaign'].unique())

    campaigns_id = [campaign.split('n:')[2] for campaign in campaigns]
    campaign_list = []

    for id in campaigns_id:
        campaign_list.append(requests.get(f'https://api.linkedin.com/v2/adCampaignsV2/{id}', headers = header).json())
    
    campaigns_df = pd.DataFrame(campaign_list)
    creative_df['campaign_id'] = creative_df['campaign'].str[25:]

    campaigns_df = campaigns_df[['id', 'name']]
    campaigns_df.rename(columns = {column: f'campaign_{column}' for column in campaigns_df.columns}, inplace = True)

    campaigns_df['campaign_id'] = campaigns_df['campaign_id'].astype('str')

    temp_df = campaigns_df.merge(creative_df, on = 'campaign_id', how = 'left')

    if 'reference' in creative_df.columns:
        ad_names = []
        references = list(creative_df['reference'].unique())
        
        for reference in references:
            ad_names.append(requests.get(f'https://api.linkedin.com/v2/adDirectSponsoredContents/{reference}', headers = header).json())

        references_df = pd.DataFrame(ad_names)
        
        if 'name' in references_df.columns:
            references_df = references_df[['contentReference', 'name']]
            references_df.rename(columns = {'contentReference': 'reference', 'name': 'ad_name'}, inplace = True)
            temp_df = temp_df.merge(references_df, on = 'reference', how = 'left')

    df.rename(columns = {'pivotValue': 'creative_id'}, inplace = True)
    linkedin = df.merge(temp_df, on = 'creative_id', how = 'left')

    return linkedin

def fix_df(df: pd.DataFrame) -> pd.DataFrame:

    df.rename(columns = {
        'costInLocalCurrency': 'cost',
        'videoViews': 'video_views',
        'videoFirstQuartileCompletions': 'w25_views',
        'videoMidpointCompletions': 'w50_views',
        'videoThirdQuartileCompletions': 'w75_views',
        'videoCompletions': 'w100_views',
        'totalEngagements': 'post_engagement'
    }, inplace = True)

    if 'reference' in df.columns:
        df.drop(columns = ['reference'], inplace = True)
        
    df.drop(columns = [
        'campaign',
        'dateRange',
        'pivot'
    ], inplace = True)

    df['creative_id'] = df['creative_id'].str[-9:]
    df['cost'] = df['cost'].astype(float)
    df['source'] = 'linkedin'

    return df

def get_linkedin(account_name: list, start_date: str, end_date: str) -> pd.DataFrame:

    credentials = get_credentials()
    account_id = get_account(credentials, account_name)
    analytics = get_analytics(credentials, account_id, start_date, end_date)

    if analytics.empty:
            print('Não há novos dados em Linkedin.')
            linkedin = pd.DataFrame(analytics)

    else:
        linkedin = get_info(credentials, analytics)
        linkedin = fix_df(linkedin)
    
    return linkedin

if __name__ == '__main__':

    df = get_linkedin(['BB | LL | ST | EDUCAÇÃO FINANCEIRA - Nova 2'], '2022-09-22', '2022-09-25')
    df.sort_values('date', inplace = True)
    df['cost'] = df['cost'].astype(str).str.replace('.', ',')
    df.to_csv('linkedin.csv', index = False, encoding = 'latin-1')
    print(df[['cost']])
    print(df)
