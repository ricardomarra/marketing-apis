import datetime
import os
import pandas as pd
import numpy as np
import httplib2

from googleapiclient.discovery import build
from oauth2client import client
from oauth2client import file
from oauth2client import tools

def initialize_analyticsreporting():
    """Initializes the analyticsreporting service object.

    Returns:
        analytics an authorized analyticsreporting service object.
    """
    
    secret_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tokens', 'google_secret.json'))
    analytics_reporting = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tokens', 'analyticsreporting.dat'))

    scope = ['https://www.googleapis.com/auth/analytics.readonly']
    uri = ('https://analyticsreporting.googleapis.com/$discovery/rest')

    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
        secret_path, scope=scope,
        message=tools.message_if_missing(secret_path))

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storage = file.Storage(analytics_reporting)
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage)
    http = credentials.authorize(http=httplib2.Http())

    # Build the service object.
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=uri)

    return analytics

def get_overview(analytics, view_id: str, metrics: list, dimensions: list, campaigns:list, start_date: str, end_date: str) -> pd.DataFrame:

    date_diff = (pd.to_datetime(end_date).date() - pd.to_datetime(start_date).date()).days
    page_size = 200*(date_diff) if date_diff > 10 else 1000

    response = analytics.reports().batchGet(
        body={
            'reportRequests': [
            {
            'viewId': view_id,
            'pageSize': page_size,
            'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
            'metrics': metrics,
            'dimensions': dimensions,
            'dimensionFilterClauses': [
                {
                    'operator': 'OR',
                    'filters': [
                        {
                            'dimensionName': 'ga:campaign',
                            'operator': 'IN_LIST',
                            'expressions': campaigns
                        }
                    ]
                }
            ]
            }]
        }
    ).execute()

    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = [column.get('name', {}) for column in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]
        rows = report.get('data', {}).get('rows', [])
        final_row = []

        for row in rows:
            dimensions = row.get('dimensions', [])
            metrics = row.get('metrics', [])[0].get('values', {})
            row_dict = {}

            for header, dimension in zip(dimensionHeaders, dimensions):
                row_dict[header[3:].lower()] = dimension

            for metricHeader, metric in zip(metricHeaders, metrics):
                row_dict[metricHeader[3:].lower()] = metric
        
            final_row.append(row_dict)

    return pd.DataFrame(final_row)

def get_events(analytics, view_id: str, metrics: list, dimensions: list, campaigns:list, start_date: str, end_date: str) -> pd.DataFrame:
    
    date_diff = (pd.to_datetime(end_date).date() - pd.to_datetime(start_date).date()).days
    page_size = 2000*(date_diff) if date_diff > 1 else 2000

    response = analytics.reports().batchGet(
        body={
            'reportRequests': [
            {
            'viewId': view_id,
            'pageSize': page_size,
            'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
            'metrics': metrics,
            'dimensions': dimensions,
            'dimensionFilterClauses': [
                {
                    'operator': 'OR',
                    'filters': [
                        {
                            'dimensionName': 'ga:campaign',
                            'operator': 'IN_LIST',
                            'expressions': campaigns
                        }
                    ]
                },
                {
                    'operator': 'AND',
                    'filters': [
                        {
                            'dimensionName': 'ga:eventaction',
                            'not': True,
                            'operator': 'IN_LIST',
                            'expressions': ['scroll']
                        }
                    ]
                },
                {
                    'operator': 'AND',
                    'filters': [
                        {
                            'dimensionName': 'ga:eventcategory',
                            'not': True,
                            'operator': 'IN_LIST',
                            'expressions': ['barra-cookies', 'login']
                        }
                    ]
                }
            ]
            }]
        }
    ).execute()

    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = [column.get('name', {}) for column in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]
        rows = report.get('data', {}).get('rows', [])
        final_row = []

        if not rows:
            dimensions = {column: '' for column in dimensionHeaders}
            metrics = {metric: 0 for metric in metricHeaders}
            dimensions.update(metrics)
            empty_df = pd.DataFrame(dimensions, index = [0])
            empty_df.rename(columns = {column: column[3:].lower() for column in empty_df.columns}, inplace = True)

            empty_df.loc[0, 'date'] = (datetime.datetime.today().date() - datetime.timedelta(days = 1)).strftime('%Y%m%d')
            print('Não há dados de eventos!')
            return empty_df

        for row in rows:
            dimensions = row.get('dimensions', [])
            metrics = row.get('metrics', [])[0].get('values', {})
            row_dict = {}

            for header, dimension in zip(dimensionHeaders, dimensions):
                row_dict[header[3:].lower()] = dimension

            for metricHeader, metric in zip(metricHeaders, metrics):
                row_dict[metricHeader[3:].lower()] = metric
        
            final_row.append(row_dict)

    return pd.DataFrame(final_row)

def bm_get_report(analytics, view_id: str, campaigns:list, start_date: str, end_date: str) -> tuple:
     
    dimensions_overview = [
        'date', 
        'campaign', 
        'source', 
        'medium', 
        'adContent', 
        'adwordsCampaignID', 
        'adwordsCreativeID'
        ]

    metrics_overview = [
        'sessions', 
        'users', 
        'newUsers', 
        'percentNewSessions', 
        'bounces', 
        'sessionDuration'
        ]
    
    dimensions_event = [
        'date', 
        'adwordsCreativeID', 
        'campaign', 
        'source', 
        'medium', 
        'adContent', 
        'eventCategory',
        'eventAction', 
        'eventLabel'
        ]

    metrics_event = ['totalEvents', 'uniqueEvents', 'sessionsWithEvent']

    dimensions_overview_body = [{'name': f'ga:{dimension}'} for dimension in dimensions_overview]
    metrics_overview_body = [{'expression': f'ga:{metric}'} for metric in metrics_overview]

    dimensions_event_body = [{'name': f'ga:{dimension}'} for dimension in dimensions_event]
    metrics_event_body = [{'expression': f'ga:{metric}'} for metric in metrics_event]
    
    bm_overview = get_overview(
        analytics, view_id, 
        metrics_overview_body, dimensions_overview_body, 
        campaigns, start_date, end_date)

    bm_events = get_events(
        analytics, view_id, 
        metrics_event_body, dimensions_event_body, 
        campaigns, start_date, end_date)
    

    bm_overview.rename(columns = {
        'adcontent': 'content',
        'adwordscampaignid': 'campaign_id', 
        'adwordscreativeid': 'ad_id'
        }, inplace = True)

    bm_events.rename(columns = {
        'adcontent': 'content',
        'adwordscreativeid': 'ad_id'
        }, inplace = True)

    
    numeric_float = ['percentnewsessions', 'sessionduration']
    ov_numeric_int = ['sessions', 'users', 'newusers', 'bounces']
    ov_strings = [column for column in bm_overview.columns if column not in (numeric_float + ov_numeric_int)]

    bm_overview[numeric_float] = bm_overview[numeric_float].astype(float)
    bm_overview[ov_numeric_int] = bm_overview[ov_numeric_int].astype(int)
    bm_overview[ov_strings] = bm_overview[ov_strings].astype(str)
    bm_overview['date'] = pd.to_datetime(bm_overview['date'], format = '%Y%m%d')

    bm_overview['newsessions'] = np.round((bm_overview['percentnewsessions'] / 100) * bm_overview['sessions'])
    bm_overview['avgsessionduration'] = np.round(bm_overview['sessionduration'] / bm_overview['sessions'])

    bm_overview.drop(columns = ['percentnewsessions', 'sessionduration'], inplace = True)

    ev_numeric_int = ['totalevents', 'uniqueevents', 'sessionswithevent']
    ev_strings = [column for column in bm_events.columns if column not in ev_numeric_int]

    bm_events[ev_numeric_int] = bm_events[ev_numeric_int].astype(int)
    bm_events[ev_strings] = bm_events[ev_strings].astype(str)
    bm_events['date'] = pd.to_datetime(bm_events['date'], format = '%Y%m%d')
    
    return bm_overview, bm_events
    
def cm_get_report(analytics, view_id: str, campaigns:list, start_date: str, end_date: str) -> tuple:

    dimensions_overview = [
        'date', 
        'dcmClickCampaign', 
        'dcmClickSite',
        'dcmClickSitePlacement', 
        'dcmClickCreative', 
        'dcmClickSitePlacementId',
        'dcmClickAdId', 
        'dcmClickCreativeId'
        ]

    metrics_overview = [
        'sessions', 
        'users', 
        'newUsers', 
        'percentNewSessions', 
        'bounces', 
        'sessionDuration'
        ]
    
    dimensions_event = [
        'date', 
        'dcmClickCampaign', 
        'dcmClickSite', 
        'dcmClickAdId',
        'dcmClickCreative', 
        'dcmClickCreativeId',
        'eventCategory', 
        'eventAction', 
        'eventLabel'
        ]

    metrics_event = ['totalEvents', 'uniqueEvents', 'sessionsWithEvent']

    dimensions_overview_body = [{'name': f'ga:{dimension}'} for dimension in dimensions_overview]
    metrics_overview_body = [{'expression': f'ga:{metric}'} for metric in metrics_overview]

    dimensions_event_body = [{'name': f'ga:{dimension}'} for dimension in dimensions_event]
    metrics_event_body = [{'expression': f'ga:{metric}'} for metric in metrics_event]

    cm_overview = get_overview(
        analytics, view_id, 
        metrics_overview_body, dimensions_overview_body, 
        campaigns, start_date, end_date)

    cm_events = get_events(
        analytics, view_id, 
        metrics_event_body, dimensions_event_body, 
        campaigns, start_date, end_date)

    cm_overview.rename(columns = {
        'dcmclickcampaign': 'campaign', 
        'dcmclicksite': 'source', 
        'dcmclicksiteplacement': 'cm_placement',
        'dcmclicksiteplacementid': 'cm_placement_id',
        'dcmclickcreative': 'cm_creative',
        'dcmclickadid': 'ad_id',
        'dcmclickcreativeid': 'cm_creative_id'}, inplace = True)

    cm_events.rename(columns = {
        'dcmclickcampaign': 'campaign', 
        'dcmclicksite': 'source', 
        'dcmclickcreative': 'cm_creative',
        'dcmclickadid': 'ad_id',
        'dcmclickcreativeid': 'cm_creative_id'}, inplace = True)


    numeric_float = ['percentnewsessions', 'sessionduration']
    ov_numeric_int = ['sessions', 'users', 'newusers', 'bounces']
    ov_strings = [column for column in cm_overview.columns if column not in (numeric_float + ov_numeric_int)]

    cm_overview[numeric_float] = cm_overview[numeric_float].astype(float)
    cm_overview[ov_numeric_int] = cm_overview[ov_numeric_int].astype(int)
    cm_overview[ov_strings] = cm_overview[ov_strings].astype(str)
    cm_overview['date'] = pd.to_datetime(cm_overview['date'], format = '%Y%m%d')

    cm_overview['newsessions'] = np.round((cm_overview['percentnewsessions'] / 100) * cm_overview['sessions'])
    cm_overview['avgsessionduration'] = np.round(cm_overview['sessionduration'] / cm_overview['sessions'])

    cm_overview.drop(columns = ['percentnewsessions', 'sessionduration'], inplace = True)

    ev_numeric_int = ['totalevents', 'uniqueevents', 'sessionswithevent']
    ev_strings = [column for column in cm_events.columns if column not in ev_numeric_int]

    cm_events[ev_numeric_int] = cm_events[ev_numeric_int].astype(int)
    cm_events[ev_strings] = cm_events[ev_strings].astype(str)
    cm_events['date'] = pd.to_datetime(cm_events['date'], format = '%Y%m%d')

    return cm_overview, cm_events

def manipulate_bm(bm_overview: pd.DataFrame, bm_events: pd.DataFrame):

    bm_overview.loc[bm_overview['content'] == '(not set)', 'content'] = ''
    bm_overview.loc[bm_overview['ad_id'] == '(not set)', 'ad_id'] = ''
    
    bm_events.loc[bm_events['content'] == '(not set)', 'content'] = ''
    bm_events.loc[bm_events['ad_id'] == '(not set)', 'ad_id'] = ''

    bm_overview['ident_id'] = ''
    bm_events['ident_id'] = ''

    bm_overview.loc[bm_overview['content'] != '', 'ident_id'] = bm_overview.loc[bm_overview['content'] != '', 'content']  + bm_overview.loc[bm_overview['content'] != '', 'source'] 
    bm_events.loc[bm_events['content'] != '', 'ident_id'] = bm_events.loc[bm_events['content'] != '', 'content']  + bm_events.loc[bm_events['content'] != '', 'source'] 

    bm_overview.loc[bm_overview['ad_id'] != '', 'ident_id'] = bm_overview.loc[bm_overview['ad_id'] != '', 'ad_id']
    bm_events.loc[bm_events['ad_id'] != '', 'ident_id'] = bm_events.loc[bm_events['ad_id'] != '', 'ad_id']

    if bm_overview.duplicated(['date', 'content', 'source']).all():
        writer = pd.ExcelWriter('error_ga.xlsx')
    
        bm_overview[bm_overview.duplicated(['date', 'content', 'source'], keep = False)].to_excel(writer, sheet_name = 'ov_duplicated')
        bm_overview.drop(bm_overview[bm_overview.duplicated(['date', 'content', 'source'], keep = False)].index, inplace = True)
        bm_overview.reset_index(drop = True, inplace = True)

        writer.save()

    if bm_events.duplicated(['date', 'content', 'source']).all():
        writer = pd.ExcelWriter('error_ga.xlsx')

        bm_events[bm_events.duplicated(['date', 'content', 'source'], keep = False)].to_excel(writer, sheet_name = 'ev_duplicated')
        bm_events.drop(bm_events[bm_events.duplicated(['date', 'content', 'source'], keep = False)].index, inplace = True)
        bm_events.reset_index(drop = True, inplace = True)

        writer.save()

    bm_overview['ov_ev_join'] = ''
    bm_events['ov_ev_join'] = ''

    bm_overview['ov_ev_join'] = bm_overview['date'].astype(str) + '__' + bm_overview['source'] + '__' + bm_overview['medium'] + '__' + bm_overview['ident_id'].astype(str)
    bm_events['ov_ev_join'] = bm_events['date'].astype(str) + '__' + bm_events['source'] + '__' + bm_events['medium'] + '__' + bm_events['ident_id'].astype(str)

    if bm_events['ov_ev_join'].isin(bm_overview['ov_ev_join']).all():
        
        print(bm_events[~bm_events['ov_ev_join'].isin(bm_overview['ov_ev_join'])].ov_ev_join.unique())

    bm_events_grouped = bm_events.groupby(by = 'ov_ev_join', as_index = False).sum()[['ov_ev_join', 'totalevents', 'uniqueevents', 'sessionswithevent']]
    bm_overview = bm_overview.merge(bm_events_grouped, on = 'ov_ev_join', how = 'left')

    bm_overview.fillna(0, inplace = True)

    bm_overview.drop(columns = ['ident_id', 'ov_ev_join'], inplace = True)
    bm_events.drop(columns = ['ident_id', 'ov_ev_join'], inplace = True)
    
    return bm_overview, bm_events

def manipulate_cm(cm_overview: pd.DataFrame, cm_events: pd.DataFrame) -> pd.DataFrame:
    
    cm_overview.loc[cm_overview['cm_creative_id'] == '(not set)', 'cm_creative_id'] = ''
    cm_overview.loc[cm_overview['ad_id'] == '(not set)', 'ad_id'] = ''
    
    cm_events.loc[cm_events['cm_creative_id'] == '(not set)', 'cm_creative_id'] = ''
    cm_events.loc[cm_events['ad_id'] == '(not set)', 'ad_id'] = ''

    if cm_overview.duplicated(['date', 'cm_creative_id', 'source']).all():
        writer = pd.ExcelWriter('error_ga.xlsx')
    
        cm_overview[cm_overview.duplicated(['date', 'content', 'source'], keep = False)].to_excel(writer, sheet_name = 'ov_duplicated')
        cm_overview.drop(cm_overview[cm_overview.duplicated(['date', 'content', 'source'], keep = False)].index, inplace = True)
        cm_overview.reset_index(drop = True, inplace = True)

        writer.save()

    if cm_events.duplicated(['date', 'cm_creative_id', 'source']).all():
        writer = pd.ExcelWriter('error_ga.xlsx')

        cm_events[cm_events.duplicated(['date', 'content', 'source'], keep = False)].to_excel(writer, sheet_name = 'ev_duplicated')
        cm_events.drop(cm_events[cm_events.duplicated(['date', 'content', 'source'], keep = False)].index, inplace = True)
        cm_events.reset_index(drop = True, inplace = True)

        writer.save()

    cm_overview['ov_ev_join'] = ''
    cm_events['ov_ev_join'] = ''

    cm_overview['ov_ev_join'] = cm_overview['date'].astype(str) + '__' + cm_overview['cm_creative_id']
    cm_events['ov_ev_join'] = cm_events['date'].astype(str) + '__' + cm_events['cm_creative_id']

    if cm_events['ov_ev_join'].isin(cm_overview['ov_ev_join']).all():
        
        print(cm_events[~cm_events['ov_ev_join'].isin(cm_overview['ov_ev_join'])].ov_ev_join.unique())

    cm_events_grouped = cm_events.groupby(by = 'ov_ev_join', as_index = False).sum()[['ov_ev_join', 'totalevents', 'uniqueevents', 'sessionswithevent']]
    cm_overview = cm_overview.merge(cm_events_grouped, on = 'ov_ev_join', how = 'left')

    cm_overview.fillna(0, inplace = True)

    cm_overview.drop(columns = ['ov_ev_join'], inplace = True)
    cm_events.drop(columns = ['ov_ev_join'], inplace = True)

    return cm_overview, cm_events


def get_googleAnalytics_bm(view_id: str, campaigns_bm: list, start_date: str, end_date: str):

    analytics = initialize_analyticsreporting()

    bm_overview, bm_events = bm_get_report(analytics, view_id, campaigns_bm, start_date, end_date)
    bm_overview, bm_events = manipulate_bm(bm_overview, bm_events)

    return bm_overview, bm_events

def get_googleAnalytics_cm(view_id: str, campaigns_cm: list, start_date: str, end_date: str):

    analytics = initialize_analyticsreporting()

    cm_overview, cm_events = cm_get_report(analytics, view_id, campaigns_cm, start_date, end_date)
    cm_overview, cm_events = manipulate_cm(cm_overview, cm_events)

    return cm_overview, cm_events

def main():

    analytics = initialize_analyticsreporting()

    start_date = '2022-04-25'
    end_date = '2022-05-08'

    view_id = '113545301'
    campaigns_cm = ['datas-comemorativas_dt-diadasmaes']

    cm_overview, cm_events = get_googleAnalytics_cm(view_id, campaigns_cm, start_date, end_date)


    # campaigns_bm = ['2022_an_univer', 
    # 'BB | LL | AN | Apoio - Universitários | Awareness | YouTube | Alcance | CPM | InStream Não Pulável',
    # 'BB | LL | AN | Apoio - Universitários | Engajamento | YouTube | CPM | InStream Não Pulável', 
    # 'BB | LL | AN | Apoio - Universitários | Alcance | YouTube | CPM | Bumper | LOLLA AUDIÊNCIAS',
    # 'BB | LL | AN | Apoio - Universitários | Tráfego | CPC | Display',
    # 'BB | LL | AN | Apoio - Universitários | Alcance | YouTube | CPM | InStream Não Pulável | LOLLA AUDIÊNCIAS',
    # 'BB | LL | AN | Apoio - Universitários | Awareness | YouTube | Alcance | CPM | Bumper Ads']

    # bm_overview, bm_events = bm_get_report(analytics, view_id, campaigns_bm, start_date, end_date)
    # bm_overview, bm_events = manipulate_bm(bm_overview, bm_events)

    writer = pd.ExcelWriter('google-analytics_data.xlsx')
    cm_overview.to_excel(writer, index = False, sheet_name = 'ga_overview')
    cm_events.to_excel(writer, index = False, sheet_name = 'ga_events')
    writer.save()

# if __name__ == '__main__':
#    main()