from sqlite3 import Timestamp
from twitter_ads.client import Client
from twitter_ads.campaign import Campaign
from twitter_ads.enum import ENTITY_STATUS, METRIC_GROUP, TWEET_TYPE
from twitter_ads.http import Request
from twitter_ads.creative import PromotedTweet, Card, CardsFetch, Tweets
from twitter_ads.utils import split_list

import datetime
import time
import os
import json
import pandas as pd
import numpy as np


def get_credentials() -> dict:

    credentials_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tokens', 'twitter_credentials.json'))

    with open(credentials_path, 'r') as f:
        credentials = json.load(f)

    return credentials

def get_client() -> Client:

    credentials = get_credentials()

    client = Client(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'], 
                    credentials['ACCESS_TOKEN'], credentials['ACCESS_TOKEN_SECRET'])

    return client 

def get_tweets_basic_info(account: Client.accounts) -> pd.DataFrame:
    '''Pega as informações básicas de todos os tweets (ids, infos de campanha)'''
    
    tweets = list(account.promoted_tweets(with_draft = 'true', with_deleted = 'true'))
    line_items = list(account.line_items(with_draft = 'true', with_deleted = 'true'))
    campaigns = list(account.campaigns(with_draft = 'true', with_deleted = 'true'))

    tweet_dict = {
        'id': [],
        'tweet_id': [],
        'line_item_id': [],
        'entity_status': [],
        'created_at': [],
        'updated_at': []
    }

    line_items_dict = {
        'line_item_id': [],
        'campaign_id': [],
        'campaign_objective': []
    }

    campaign_dict = {
        'campaign_id': [],
        'campaign_name': []
    }

    for tweet in tweets:
        tweet_dict['id'].append(tweet.id)
        tweet_dict['tweet_id'].append(tweet.tweet_id)
        tweet_dict['line_item_id'].append(tweet.line_item_id)
        tweet_dict['entity_status'].append(tweet.entity_status)
        tweet_dict['created_at'].append(tweet.created_at)
        tweet_dict['updated_at'].append(tweet.updated_at)

    for line_item in line_items:
        line_items_dict['line_item_id'].append(line_item.id)
        line_items_dict['campaign_id'].append(line_item.campaign_id)
        line_items_dict['campaign_objective'].append(line_item.objective)
        
    for campaign in campaigns:
        campaign_dict['campaign_id'].append(campaign.id)
        campaign_dict['campaign_name'].append(campaign.name)
        

    tweets_df = pd.DataFrame(tweet_dict)
    line_items_df = pd.DataFrame(line_items_dict)
    campaigns_df = pd.DataFrame(campaign_dict)

    tweets_df = tweets_df.merge(line_items_df, on = 'line_item_id', how = 'left')
    tweets_df = tweets_df.merge(campaigns_df, on = 'campaign_id', how = 'left')

    return tweets_df

def apply_campaign_filter(df: pd.DataFrame, campaigns: list) -> pd.DataFrame:
    '''Aplica o filtro de campanha.'''

    # em construção
    filtered_df = df[df['campaign_name'].str.replace(' ', '').isin([campaign.replace(' ', '') for campaign in campaigns])]
    filtered_df.reset_index(drop = True, inplace = True)

    return filtered_df

def get_cards(df: pd.DataFrame, account: Client.accounts, client: Client) -> pd.DataFrame:
    '''Pega as informações do card dos tweets e adiciona ao dataframe básico. (texto, url)'''

    card_dict = {
        'tweet_id': [],
        'card_uri': [],
        'post_text': []
    }

    non_card_dict = {
        'tweet_id': [],
        'card_uri': [],
        'post_text': [],
        'ad_name': [],
        'media_url': [],
        'destination_url': []
    }

    card_info = {
        'ad_name': [],
        'media_url': [],
        'card_uri': [],
        'destination_url': []
    }

    tweet_ids = list(set(df['tweet_id']))

    tweets = list(Tweets.all(account, tweet_type = TWEET_TYPE.PUBLISHED, tweet_ids = tweet_ids))

    for tweet in tweets:
        if 'card_uri' in tweet.keys():
            card_dict['tweet_id'].append(tweet['tweet_id'])
            card_dict['card_uri'].append(tweet['card_uri'])
            card_dict['post_text'].append(tweet['full_text'])
        else:
            non_card_dict['tweet_id'].append(tweet['tweet_id'])
            non_card_dict['post_text'].append(tweet['full_text'])
            non_card_dict['ad_name'].append(tweet['name'])
            non_card_dict['media_url'].append(tweet['entities']['media'][0]['media_url_https'])
            non_card_dict['card_uri'].append('')
            non_card_dict['destination_url'].append('')

    tweet_cards = pd.DataFrame(card_dict)
    tweet_non_cards = pd.DataFrame(non_card_dict)

    if tweet_cards.card_uri.any():
        url = f'/11/accounts/{account.id}/cards'
        params = {'card_uris': ','.join(card_dict['card_uri'])}

        response = Request(client, 'get', url, params = params).perform().body

        for card in response['data']:
            card_info['ad_name'].append(card['name'])
            card_info['media_url'].append(card['components'][0]['media_metadata'][card['components'][0]['media_key']]['url'])
            card_info['destination_url'].append(card['components'][1]['destination']['url'])
            card_info['card_uri'].append(card['card_uri'])

    cards_df = pd.DataFrame(card_info)

    tweet_cards_df = tweet_cards.merge(cards_df, how = 'left', on = 'card_uri')
    df_cards_info = pd.concat([tweet_cards_df, tweet_non_cards], axis = 0)

    cards_final = df.merge(df_cards_info, how = 'left', on = 'tweet_id')

    username = tweets[0]['user']['screen_name']

    cards_final['source_url'] = 'https://twitter.com/' + username + '/status/' + cards_final['tweet_id']

    return cards_final

def get_metrics(df: pd.DataFrame, account: Client.accounts, start_date: Timestamp, end_date: Timestamp):
    '''Puxa todas as métricas do tweet, por dia.'''

    entity_ids = list(df['id'])

    metric_groups = [
        METRIC_GROUP.ENGAGEMENT, METRIC_GROUP.BILLING, 
        METRIC_GROUP.MEDIA, METRIC_GROUP.VIDEO
        ]

    dates = pd.Series(pd.date_range(start = start_date, end = end_date))
    dates = dates.groupby(np.arange(len(dates))//30).agg(['first', 'last'])

    async_data_total = []

    for init_date, end_date in dates.itertuples(index = False):
        init_temp = init_date.strftime('%Y-%m-%d')
        end_temp = end_date.strftime('%Y-%m-%d')
        
        start_time = datetime.datetime.strptime('{}{}'.format(init_temp, 'T00:00:00Z'), '%Y-%m-%dT%H:%M:%SZ')
        end_time = datetime.datetime.strptime('{}{}'.format(end_temp, 'T00:00:00Z'), '%Y-%m-%dT%H:%M:%SZ') + datetime.timedelta(days=1)
        
        job_ids = []
        for chunk in split_list(entity_ids, 20):
            job_ids.append(PromotedTweet.queue_async_stats_job(account, chunk, metric_groups, granularity = 'DAY',
                                                        start_time = start_time, end_time = end_time).id)
        time.sleep(30)
        async_stats_job_results = list(PromotedTweet.async_stats_job_result(account, job_ids = job_ids))

        while (pd.Series([trabalho.status for trabalho in async_stats_job_results]) != 'SUCCESS').any():
            print('PROCESSANDO')
            time.sleep(30)
            async_stats_job_results = list(PromotedTweet.async_stats_job_result(account, job_ids = job_ids))

        async_data = []
        for result in async_stats_job_results:
            async_data.append(PromotedTweet.async_stats_job_data(account, url = result.url))
            
        async_data_total = async_data_total + async_data
        
        print(f'Coletado de {init_temp} até {end_temp}')

    metrics_list = []

    for report in async_data_total:
        for i in range(len(report['data'])):
            try:
                temp = pd.DataFrame(report['data'][i]['id_data'][0]['metrics'])
            except:
                temp = pd.DataFrame(report['data'][i]['id_data'][0]['metrics'], index = [0])
        
            temp['id'] = report['data'][i]['id']
            temp['date'] = pd.Series(pd.date_range(report['request']['params']['start_time'], report['request']['params']['end_time']))
            temp.fillna(0, inplace = True)

            metrics_list.append(temp)

    metrics_df = pd.concat(metrics_list, sort = False, ignore_index = True)
    
    metrics_df.drop(metrics_df[metrics_df.sum(axis = 1) == 0].index, inplace = True)
    metrics_df.reset_index(drop = True, inplace = True)

    final_df = df.merge(metrics_df, how = 'inner', on = 'id')

    return final_df

def fix_df(df: pd.DataFrame) -> pd.DataFrame:

    df['cost'] = df['billed_charge_local_micro']/1000000

    df['date'] = df['date'].dt.tz_localize(None).dt.date

    columns_to_drop = [
        'id', 'line_item_id', 'entity_status', 'created_at', 'updated_at', 'card_uri', 'tweets_send', 'qualified_impressions',
        'media_engagements', 'follows', 'video_3s100pct_views', 'app_clicks', 'retweets', 'video_cta_clicks', 'unfollows',
        'likes', 'video_content_starts', 'media_views', 'card_engagements', 'video_6s_views', 'poll_card_vote',
        'replies', 'video_15s_views', 'url_clicks', 'billed_engagements', 'carousel_swipes', 'billed_charge_local_micro'
    ]

    df.drop(columns = columns_to_drop, inplace = True)

    rename_videos = {
        'video_views_25': 'w25_views',
        'video_views_50': 'w50_views',
        'video_views_75': 'w75_views',
        'video_views_100': 'w100_views',
        'video_total_views': 'video_views'
    }

    df.rename(columns = rename_videos, inplace = True)
    df['source'] = ''
    df['medium'] = ''
    df['campaign'] = ''
    df['content'] = ''

    df.rename(columns = {'campaign_objective': 'objective', 'engagements': 'post_engagement'}, inplace = True)

    df['destination_url'].fillna('', inplace = True)

    no_content = df[~df['destination_url'].str.contains('content')]
    no_content['source'] = 'twitter'
    df.drop(df[~df['destination_url'].str.contains('content')].index, inplace = True)

    if df.shape[0] > 0:

        df['source'] = df['destination_url'].str.replace('&', '').str.split('utm_', expand = True)[1].str.split('=', expand = True)[1]
        df['medium'] = df['destination_url'].str.replace('&', '').str.split('utm_', expand = True)[2].str.split('=', expand = True)[1]
        df['campaign'] = df['destination_url'].str.replace('&', '').str.split('utm_', expand = True)[3].str.split('=', expand = True)[1]
        df['content'] = df['destination_url'].str.replace('&', '').str.split('utm_', expand = True)[5].str.split('=', expand = True)[1]

    df = pd.concat([df, no_content], axis = 0, ignore_index = True)

    return df

def get_twitter(campaigns: list, start_date: str, end_date: str):

    crendentials = get_credentials()
    client = get_client()
    account = client.accounts(crendentials['ACCOUNT_ID'])

    basic_info_df = get_tweets_basic_info(account)

    filtered_df = apply_campaign_filter(basic_info_df, campaigns)
    cards_df = get_cards(filtered_df, account, client)

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    final_df = get_metrics(cards_df, account, start_date, end_date)
    df = fix_df(final_df)

    return df

def main():

    campaigns = ['DT | Dia Das Mães | Abril | AQUECIMENTO', 'DT | Dia Das Mães | Abril,DT | Dia Das Mães | Abril', 'DT | Dia Das Mães | Maio']

    start_date = '2022-04-25'
    end_date = '2022-05-08'

    df = get_twitter(campaigns, start_date, end_date)
    print(df.columns)

# if __name__ == '__main__':
#     main()