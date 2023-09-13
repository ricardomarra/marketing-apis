from google.ads.googleads.client import GoogleAdsClient

import os

import numpy as np
import pandas as pd

def start_service() -> GoogleAdsClient:

    google_ads_token = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tokens', 'google-ads.yaml'))
    client = GoogleAdsClient.load_from_storage(google_ads_token)

    return client.get_service("GoogleAdsService")


def get_accounts_ids(service: GoogleAdsClient, ad_accounts: list) -> list:

    query = """
        SELECT
          customer_client.descriptive_name,
          customer_client.id
        FROM customer_client
        WHERE customer_client.level <= 1"""

    stream = service.search_stream(customer_id = '4157458866', query = query)

    accounts_dict = {
        'name': [],
        'id': [] 
        }

    for batch in stream:
        for row in batch.results:
            accounts_dict['name'].append(row.customer_client.descriptive_name)
            accounts_dict['id'].append(row.customer_client.id)

    accounts = pd.DataFrame(accounts_dict)
    accounts['id'] = accounts['id'].astype(str)

    accounts_ids = list(accounts.loc[accounts['name'].isin(ad_accounts), 'id'].unique())

    return accounts_ids
    

def get_report(service: GoogleAdsClient, ad_accounts: list, start_date: str, end_date: str) -> pd.DataFrame:

    accounts_ids = get_accounts_ids(service, ad_accounts)
    reports = []

    for account_id in accounts_ids:
        
        query = f"""
        SELECT
            segments.date,
            customer.descriptive_name,
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group_ad.ad.name,
            ad_group_ad.ad.id,
            ad_group_ad.ad.final_urls,
            ad_group_ad.ad.url_custom_parameters,
            metrics.cost_micros,
            metrics.clicks,
            metrics.conversions,
            metrics.impressions,
            metrics.interactions,
            metrics.video_views,
            metrics.video_quartile_p25_rate,
            metrics.video_quartile_p50_rate,
            metrics.video_quartile_p75_rate,
            metrics.video_quartile_p100_rate
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """

        stream = service.search_stream(customer_id = account_id, query = query)

        report = {
            'account_name': [],
            'campaign_id': [],
            'campaign': [],
            'adset_id': [],
            'adset_name': [],
            'ad_name': [],
            'ad_id': [],
            'destination_url': [],
            'content': [],
            'impressions': [],
            'post_engagement': [],
            'date': [],
            'clicks': [],
            'cost': [],
            'results': [],
            'video_views': [],
            'w25_views': [],
            'w50_views': [],
            'w75_views': [],
            'w100_views': []
        }

        for batch in stream:
            for row in batch.results:
                report['account_name'].append(row.customer.descriptive_name)
                report['campaign_id'].append(row.campaign.id)
                report['campaign'].append(row.campaign.name)
                report['date'].append(row.segments.date)
                report['adset_id'].append(row.ad_group.id)
                report['adset_name'].append(row.ad_group.name)
                report['ad_id'].append(row.ad_group_ad.ad.id)
                report['content'].append(row.ad_group_ad.ad.url_custom_parameters[0].value) if len(row.ad_group_ad.ad.url_custom_parameters) > 0 else report['content'].append('')
                report['ad_name'].append(row.ad_group_ad.ad.name)
                report['destination_url'].append(row.ad_group_ad.ad.final_urls[0]) if len(row.ad_group_ad.ad.final_urls) > 0 else report['destination_url'].append('')
                report['impressions'].append(row.metrics.impressions)
                report['post_engagement'].append(row.metrics.interactions)
                report['clicks'].append(row.metrics.clicks)
                report['cost'].append(row.metrics.cost_micros)
                report['results'].append(row.metrics.conversions)
                report['video_views'].append(row.metrics.video_views)
                report['w25_views'].append(row.metrics.video_quartile_p25_rate)
                report['w50_views'].append(row.metrics.video_quartile_p50_rate)
                report['w75_views'].append(row.metrics.video_quartile_p75_rate)
                report['w100_views'].append(row.metrics.video_quartile_p100_rate)

        report_df = pd.DataFrame(report).sort_values('date', ignore_index = True)
        report_df['cost'] = report_df['cost'] / 10**6
        report_df[[column for column in report_df.columns if '_id' in column]] = report_df[[column for column in report_df.columns if '_id' in column]].astype(str)
        
        reports.append(report_df)
    
    df = pd.concat(reports)

    df['date'] = pd.to_datetime(df['date'])

    df['w25_views'] = np.round(df['w25_views'] * df['impressions'])
    df['w50_views'] = np.round(df['w50_views'] * df['impressions'])
    df['w75_views'] = np.round(df['w75_views'] * df['impressions'])
    df['w100_views'] = np.round(df['w100_views'] * df['impressions'])

    return df


def get_googleAds(accounts: list, start_date: str, end_date: str) -> pd.DataFrame:

    service = start_service()

    googleAds_df = get_report(service, accounts, start_date, end_date)

    return googleAds_df

# def main():

#     service = start_service()

#     googleAds_df = get_report(service, ['BB | LL | Apoio MC | Estilo (Antiga Conversação | Estilo)'], '2022-02-10', '2022-04-01')

#     # save_path = os.path.abspath(os.path.join(campaign_path, 'data', 'googleAds_prep.xlsx'))
    
#     print(f'Google Ads feito! Salvo em')

#     googleAds_df.to_excel('googleAds_prep.xlsx', sheet_name = 'googleAds_prep', index = False)

#     return

# if __name__ == '__main__':

#     main()