import requests
import os
import json

import pandas as pd

def get_adjust(start_date: str, end_date: str) -> pd.DataFrame:

    credentials_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tokens', 'adjust_credentials.json'))

    with open(credentials_path, 'r') as f:
        credentials = json.load(f)

    app_token = credentials['app_token']
    api_token = credentials['api_token']

    dimensions = [
        'app',
        'day',
        'partner_name',
        'network',
        'campaign_id_network',
        'campaign_network',
        'adgroup_id_network',
        'adgroup_network',
        'creative_id_network',
        'creative_network'
    ]

    metrics = [
        'impressions',
        'clicks',
        'installs',
        'sessions',
        'reattributions',
        'click_conversion_rate',
        'impression_conversion_rate'
    ]

    params = {
        'app_token': app_token,
        'date_period': f'{start_date}:{end_date}',
        'utc_offset': '-03:00',
        'dimensions': ','.join(dimensions),
        'metrics': ','.join(metrics),
        'cost_mode': 'network'
    }

    response = requests.get(
        url = 'https://dash.adjust.com/control-center/reports-service/report', 
        headers = {'Authorization': f'Bearer {api_token}'},
        params = params
        )

    adjust = pd.DataFrame(response.json()['rows'])
    df = adjust[adjust['campaign_network'].str.contains('LL')].sort_values('day')

    df.drop(columns = 'attr_dependency', inplace = True)

    df.rename(columns = {column:column.replace('_network', '') for column in df.columns if '_network' in column}, inplace = True)
    df.rename(columns = {'day': 'date'}, inplace = True)

    number_int = ['impressions', 'clicks', 'installs', 'sessions', 'reattributions']
    number_float = ['click_conversion_rate', 'impression_conversion_rate']

    df[number_int] = df[number_int].astype(int)
    df[number_float] = df[number_float].astype(float)
    df['date'] = pd.to_datetime(df['date'])

    return df