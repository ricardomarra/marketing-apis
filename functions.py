import pandas as pd
import numpy as np

import datetime
import os

from api.api_adjust import get_adjust

def merge_params(bm_df: pd.DataFrame, parametric_df: pd.DataFrame, campaign_path: str, bm: str, content: bool) -> pd.DataFrame:

    param_df = parametric_df.copy()

    if bm == 'googleAds':
        param_df['source'] = param_df['canal']

    if isinstance(bm_df, pd.DataFrame):
        if bm == 'TikTok' or bm == 'Linkedin':
            bm_df.drop(columns = [column for column in (bm_df.columns.intersection(param_df.columns)) if column != 'ad_name'], inplace = True)
            merged_df = bm_df.merge(param_df, on = 'ad_name', how = 'left')

            return merged_df

        else:
            no_content_param = bm_df.loc[~bm_df['content'].isin(param_df['content']), 'content'].unique()

            if len(no_content_param) > 0:
                content_error = pd.DataFrame(no_content_param, columns = ['contents fora da parametrização'])
                error_file = os.path.abspath(os.path.join(campaign_path, f'{bm}_erro_content.xlsx'))
                content_error.to_excel(error_file , sheet_name = f'{bm}_erro_content', index = False)

            if content and bm_df.shape[0] > 0:
                bm_df.drop(columns = [column for column in (bm_df.columns.intersection(param_df.columns)) if column != 'content'], inplace = True)
                merged_df = bm_df.merge(param_df, on = 'content', how = 'left')

                return merged_df

            if ~content and bm_df.shape[0] > 0:
                if bm == 'googleAds':
                    param_df.rename(columns = {'ad_name': 'adset_name'}, inplace = True)
                    bm_df.drop(columns = [column for column in (bm_df.columns.intersection(param_df.columns)) if column != 'adset_name'], inplace = True)
                    merged_df = bm_df.merge(param_df, on = 'adset_name', how = 'left')

                else:
                    bm_df.drop(columns = [column for column in (bm_df.columns.intersection(param_df.columns)) if column != 'ad_name'], inplace = True)
                    merged_df = bm_df.merge(param_df, on = 'ad_name', how = 'left')

                return merged_df


def merge_cm_params(cm_df: pd.DataFrame, parametric_df: pd.DataFrame, campaign_path: str) -> pd.DataFrame:

    param_df = parametric_df.copy()

    no_content_param = cm_df.loc[~cm_df['cm_creative'].isin(param_df['cm_creative']), 'cm_creative'].unique()

    if len(no_content_param) > 0:
        content_error = pd.DataFrame(no_content_param, columns = ['contents fora da parametrização'])
        error_file = os.path.abspath(os.path.join(campaign_path, 'cm_erro_content.xlsx'))
        content_error.to_excel(error_file , sheet_name = 'cm_erro_content', index = False)

    param_df.drop(columns = [column for column in (cm_df.columns.intersection(param_df.columns)) if column != 'cm_creative'], inplace = True)
    merged_df = cm_df.merge(param_df, on = 'cm_creative', how = 'left')

    return merged_df

def update_adjust(campaign_path:str , init_date: datetime.time) -> pd.DataFrame:

    yesterday = datetime.datetime.today().date() - datetime.timedelta(days = 1)

    print('Atualizando Adjust')
    adjust_file = os.path.abspath(os.path.join(campaign_path, 'source', 'adjust_prep.xlsx'))
    
    if os.path.isfile(adjust_file):
        adjust_saved = pd.read_excel(adjust_file, sheet_name = 'adjust_prep', parse_dates = ['date'])

        adjust_saved[[column for column in adjust_saved.columns if '_id' in column]] = adjust_saved[[column for column in adjust_saved.columns if '_id' in column]].astype(str)
        adjust_saved['date'] = pd.to_datetime(adjust_saved['date'])
        
        adjust_init = (adjust_saved['date'].max().date() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d')
        adjust_end = yesterday.strftime('%Y-%m-%d')

        if adjust_saved['date'].max().date() == yesterday:
            print('Adjust já atualizado!')
            adjust_data = adjust_saved

        else:
            print(f'Atualizando Adjust [{adjust_init}] -> [{adjust_end}]')

            adjust_temp = get_adjust(adjust_init, adjust_end)
            adjust_data = pd.concat([adjust_saved, adjust_temp], axis = 0, ignore_index = True)
            
            adjust_data.sort_values('date', inplace = True, ignore_index = True)
            adjust_data.to_excel(adjust_file, sheet_name = 'adjust_prep', index = False)
            print('Adjust atualizado!')

    else:

        adjust_init = init_date.strftime('%Y-%m-%d')
        adjust_end = yesterday.strftime('%Y-%m-%d')

        print(f'Atualizando adjust [{adjust_init}] -> [{adjust_end}]')

        adjust_data = get_adjust(adjust_init, adjust_end)     
        adjust_data.sort_values('date', inplace = True, ignore_index = False)
        adjust_data.to_excel(adjust_file, sheet_name = 'adjust_prep', index = False)
        print('Adjust atualizado!')
        
    return adjust_data


def update_bm(campaign_path: str, init_date: datetime.date, **kwargs) -> pd.DataFrame:
    is_accounts = is_campaigns = False

    yesterday = datetime.datetime.today().date() - datetime.timedelta(days = 1)
    bm = kwargs['bm']

    if 'accounts' in kwargs.keys():
        bm_accounts = kwargs['accounts']
        is_accounts = True

    if 'campaigns' in kwargs.keys():
        bm_campaigns = kwargs['campaigns']
        is_campaigns = True

    bm_function = kwargs['bm_function']

    print(f'Atualizando {bm}.')
    bm_file = os.path.abspath(os.path.join(campaign_path, 'data', f'{bm.lower()}_prep.xlsx'))
    
    if os.path.isfile(bm_file):
        bm_saved = pd.read_excel(bm_file, sheet_name = f'{bm.lower()}_prep', parse_dates = ['date'])

        bm_saved[[column for column in bm_saved.columns if '_id' in column]] = bm_saved[[column for column in bm_saved.columns if '_id' in column]].astype(str)
        bm_saved['date'] = pd.to_datetime(bm_saved['date'])
        
        bm_init = (bm_saved['date'].max().date() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d')
        bm_end = yesterday.strftime('%Y-%m-%d')

        if bm_saved['date'].max().date() == yesterday:
            print(f'{bm} já atualizado!')
            bm_data = bm_saved
        
        else:
            print(f'Atualizando {bm} [{bm_init}] -> [{bm_end}]')

            if is_accounts:
                bm_temp = bm_function(bm_accounts, bm_init, bm_end)
            
            if is_campaigns:
                
                if bm == 'Twitter':
                    bm_temp = bm_function(bm_campaigns, bm_init, bm_end)

                if bm == 'CampaignManager':
                    bm_temp = bm_function(campaign_path, bm_campaigns, bm_init, bm_end)

            bm_data = pd.concat([bm_saved, bm_temp], axis = 0, ignore_index = True)

            bm_data.sort_values('date', inplace = True, ignore_index = False)
            bm_data.to_excel(bm_file, sheet_name = f'{bm.lower()}_prep', index = False)
            print(f'{bm} atualizado!')

    else:
        bm_init = init_date.strftime('%Y-%m-%d')
        bm_end = yesterday.strftime('%Y-%m-%d')

        print(f'Atualizando {bm} [{init_date}] -> [{bm_end}]')
        if is_accounts:
            bm_data = bm_function(bm_accounts, bm_init, bm_end)
            
        if is_campaigns:
            if bm == 'Twitter':
                bm_data = bm_function(bm_campaigns, bm_init, bm_end)

            if bm == 'CampaignManager':
                bm_data = bm_function(campaign_path, bm_campaigns, bm_init, bm_end)
                
        bm_data.sort_values('date', inplace = True, ignore_index = False)
        bm_data.to_excel(bm_file, sheet_name = f'{bm.lower()}_prep', index = False)
        print(f'{bm} atualizado!')

    return bm_data

def update_ga(campaign_path: str, view_id: str, init_date: datetime.date, **kwargs) -> pd.DataFrame:

    yesterday = datetime.datetime.today().date() - datetime.timedelta(days = 1)
    ga = kwargs['ga']

    if ga == 'bm':
        campaigns = kwargs['campaigns']

    if ga == 'cm':
        campaigns = kwargs['campaigns']

    ga_function = kwargs['ga_function']

    print(f'Atualizando Google Analytics [{ga.upper()}].')
    ga_file = os.path.abspath(os.path.join(campaign_path, 'data', f'{ga}_googleAnalytics_prep.xlsx'))
    
    if os.path.isfile(ga_file):
        ga_ov_saved = pd.read_excel(ga_file, sheet_name = 'ga_overview')
        ga_ev_saved = pd.read_excel(ga_file, sheet_name = 'ga_events')

        ga_ov_saved['ad_id'].fillna(0, inplace = True)
        ga_ev_saved['ad_id'].fillna(0, inplace = True)

        ga_ov_saved['ad_id'] = ga_ov_saved['ad_id'].astype(np.int64).astype(str)
        ga_ov_saved['date'] = pd.to_datetime(ga_ov_saved['date'])
        
        ga_ev_saved['ad_id'] = ga_ev_saved['ad_id'].astype(np.int64).astype(str)
        ga_ev_saved['date'] = pd.to_datetime(ga_ev_saved['date'])

        ga_init = (ga_ov_saved['date'].max().date() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d')
        ga_end = yesterday.strftime('%Y-%m-%d')

        if ga_ov_saved['date'].max().date() == yesterday:
            print(f'Google Analytics [{ga.upper()}] já atualizado!')
            ga_ov_data = ga_ov_saved
            ga_ev_data = ga_ev_saved
        
        else:
            print(f'Atualizando Google Analytics [{ga.upper()}] [{ga_init}] -> [{ga_end}]')

            ga_ov_temp, ga_ev_temp = ga_function(view_id, campaigns, ga_init, ga_end)
            ga_ov_data = pd.concat([ga_ov_saved, ga_ov_temp], axis = 0, ignore_index = True)
            ga_ev_data = pd.concat([ga_ev_saved, ga_ev_temp], axis = 0, ignore_index = True)

            writer = pd.ExcelWriter(ga_file)
            ga_ov_data.to_excel(writer, index = False, sheet_name = 'ga_overview')
            ga_ev_data.to_excel(writer, index = False, sheet_name = 'ga_events')
            writer.save()

            print(f'Google Analytics [{ga.upper()}] atualizado!')

    else:
        
        ga_init = init_date.strftime('%Y-%m-%d')
        ga_end = yesterday.strftime('%Y-%m-%d')

        print(f'Atualizando Google Analytics [{ga.upper()}] [{ga_init}] -> [{ga_end}]')
        ga_ov_data, ga_ev_data = ga_function(view_id, campaigns, ga_init, ga_end)

        writer = pd.ExcelWriter(ga_file)
        ga_ov_data.to_excel(writer, index = False, sheet_name = 'ga_overview')
        ga_ev_data.to_excel(writer, index = False, sheet_name = 'ga_events')
        writer.save()

        print(f'Google Analytics [{ga.upper()}] atualizado!')

    return ga_ov_data, ga_ev_data

def merge_ga(pr_df: pd.DataFrame, ga_overview: pd.DataFrame, ga_events: pd.DataFrame, **kwargs) -> tuple:

    bm = kwargs['bm']

    source_dict = {
        'facebook': ['fb', 'ig'],
        'google': ['google'],
        'twitter': ['twitter'],
        'tiktok': ['tiktok'],
        'linkedin': ['linkedin']
    }

    ov_copy = ga_overview.copy()
    ev_copy = ga_events.copy()

    overview_bm = ov_copy[ov_copy['source'].isin(source_dict[bm])]
    events_bm = ev_copy[ev_copy['source'].isin(source_dict[bm])]
    
    pr_df['date'] = pd.to_datetime(pr_df['date'])
    ov_copy['date'] = pd.to_datetime(ov_copy['date'])
    ev_copy['date'] = pd.to_datetime(ev_copy['date'])
    
    bm_metrics = [column for column in pr_df.columns if pr_df[column].dtypes not in ['object', 'datetime64[ns]']]
    ga_metrics = [column for column in overview_bm.columns if ((overview_bm[column].dtypes not in ['object', 'datetime64[ns]']) and ('event' not in column))]

    if bm == 'google':
        overview_bm['ga_id'] = overview_bm['date'].astype(str) + '__' + overview_bm['ad_id']
        events_bm['ga_id'] = events_bm['date'].astype(str) + '__' + events_bm['ad_id']

        overview_bm.drop(columns = [column for column in (pr_df.columns.intersection(overview_bm.columns)) if column not in ['ad_id', 'date']], inplace = True)
        bm_join_ov = pr_df.merge(overview_bm, on = ['date', 'ad_id'], how = 'left')

        pr_df['check_join'] = ''
        overview_bm['check_join'] = ''
        
        pr_df['check_join'] = pr_df['date'].astype(str) + '__' + pr_df['ad_id']
        overview_bm['check_join'] = overview_bm['date'].astype(str) + '__' + overview_bm['ad_id']

        bm_join_ov['juncao'] = ''
        bm_join_ov.loc[~bm_join_ov['sessions'].isnull(), 'juncao'] = 'ok'
        bm_join_ov.loc[bm_join_ov['sessions'].isnull(), 'juncao'] = 'ga_ausente'

        overview_not_bm = overview_bm[~overview_bm['check_join'].isin(pr_df['check_join'])]
        overview_not_bm.drop(columns = [column for column in (pr_df.columns.intersection(overview_not_bm.columns)) if column not in ['ad_id']], inplace = True)

        overview_not_bm = overview_not_bm.merge(pr_df[~pr_df.duplicated('ad_id')], on = 'ad_id', how = 'left')
        overview_not_bm.loc[:, bm_metrics] = 0

        overview_not_bm['juncao'] = ''
        overview_not_bm.loc[overview_not_bm['impressions'].isnull(), 'juncao'] = 'erro_ga'
        overview_not_bm.loc[~overview_not_bm['impressions'].isnull(), 'juncao'] = 'bm_ausente'

        bm_gaoverview = pd.concat([bm_join_ov, overview_not_bm], axis = 0, ignore_index = True)
        bm_gaoverview.drop(columns = [column for column in events_bm[['totalevents', 'uniqueevents', 'sessionswithevent', 'ga_id']].columns.intersection(bm_gaoverview.columns) if column not in ['ga_id']], inplace = True)
        
        bm_gaevents = events_bm[['eventcategory', 'eventaction', 'eventlabel', 'totalevents', 'uniqueevents', 'sessionswithevent', 'ga_id']].merge(bm_gaoverview[~bm_gaoverview['ga_id'].isnull()], on = 'ga_id', how = 'left')

        bm_gaevents.drop(columns = bm_metrics + ga_metrics, inplace = True)

    else:
        overview_bm['ga_id'] = overview_bm['date'].astype(str) + '__' + overview_bm['source'] + '__' + overview_bm['content']
        events_bm['ga_id'] = events_bm['date'].astype(str) + '__' + events_bm['source'] + '__' + events_bm['content']
        
        overview_bm.drop(columns = [column for column in (pr_df.columns.intersection(overview_bm.columns)) if column not in ['content', 'source', 'date']], inplace = True)
        bm_join_ov = pr_df.merge(overview_bm, on = ['date', 'source', 'content'], how = 'left')

        pr_df['check_join'] = ''
        overview_bm['check_join'] = ''
        
        pr_df['check_join'] = pr_df['date'].astype(str) + '__' + pr_df['source'] + '__' + pr_df['content']
        overview_bm['check_join'] = overview_bm['date'].astype(str) + '__' + overview_bm['source'] + '__' + overview_bm['content']

        bm_join_ov['juncao'] = ''
        bm_join_ov.loc[~bm_join_ov['sessions'].isnull(), 'juncao'] = 'ok'
        bm_join_ov.loc[bm_join_ov['sessions'].isnull(), 'juncao'] = 'ga_ausente'

        overview_not_bm = overview_bm[~overview_bm['check_join'].isin(pr_df['check_join'])]
        overview_not_bm.drop(columns = [column for column in (pr_df.columns.intersection(overview_not_bm.columns)) if column not in ['source', 'content']], inplace = True)

        overview_not_bm = overview_not_bm.merge(pr_df[~pr_df.duplicated(['source', 'content'])], on = ['source', 'content'], how = 'left')
        overview_not_bm.loc[:, bm_metrics] = 0

        overview_not_bm['juncao'] = ''
        overview_not_bm.loc[overview_not_bm['impressions'].isnull(), 'juncao'] = 'erro_ga'
        overview_not_bm.loc[~overview_not_bm['impressions'].isnull(), 'juncao'] = 'bm_ausente'

        bm_gaoverview = pd.concat([bm_join_ov, overview_not_bm], axis = 0, ignore_index = True)
        bm_gaoverview.drop(columns = [column for column in events_bm[['totalevents', 'uniqueevents', 'sessionswithevent', 'ga_id']].columns.intersection(bm_gaoverview.columns) if column not in ['ga_id']], inplace = True)
        
        bm_gaevents = events_bm[['eventcategory', 'eventaction', 'eventlabel', 'totalevents', 'uniqueevents', 'sessionswithevent', 'ga_id']].merge(bm_gaoverview[~bm_gaoverview['ga_id'].isnull()], on = 'ga_id', how = 'left')

        bm_gaevents.drop(columns = bm_metrics + ga_metrics, inplace = True)

    return  bm_gaoverview, bm_gaevents

def merge_ga_cm(pr_df: pd.DataFrame, ga_overview: pd.DataFrame, ga_events: pd.DataFrame) -> tuple:

    overview_cm = ga_overview.copy()
    events_cm = ga_events.copy()

    cm_metrics = [column for column in pr_df.columns if pr_df[column].dtypes not in ['object', 'datetime64[ns]']]
    ga_metrics = [column for column in overview_cm.columns if ((overview_cm[column].dtypes not in ['object', 'datetime64[ns]']) and ('event' not in column))]

    overview_cm['cm_creative_id'] = overview_cm['cm_creative_id'].astype(int).astype(str)
    events_cm['cm_creative_id'] = events_cm['cm_creative_id'].astype(int).astype(str)

    overview_cm['ga_id'] = overview_cm['date'].astype(str) + '__' + overview_cm['source'] + '__' + overview_cm['cm_creative_id']
    events_cm['ga_id'] = events_cm['date'].astype(str) + '__' + events_cm['source'] + '__' + events_cm['cm_creative_id']

    overview_cm.drop(columns = [column for column in (pr_df.columns.intersection(overview_cm.columns)) if column not in ['cm_creative_id', 'source', 'date']], inplace = True)
    cm_join_ov = pr_df.merge(overview_cm, on = ['date', 'source', 'cm_creative_id'], how = 'left')

    pr_df['check_join'] = ''
    overview_cm['check_join'] = ''
    
    pr_df['check_join'] = pr_df['date'].astype(str) + '__' + pr_df['source'] + '__' + pr_df['cm_creative_id']
    overview_cm['check_join'] = overview_cm['date'].astype(str) + '__' + overview_cm['source'] + '__' + overview_cm['cm_creative_id']

    cm_join_ov['juncao'] = ''
    cm_join_ov.loc[~cm_join_ov['sessions'].isnull(), 'juncao'] = 'ok'
    cm_join_ov.loc[cm_join_ov['sessions'].isnull(), 'juncao'] = 'ga_ausente'

    overview_not_cm = overview_cm[~overview_cm['check_join'].isin(pr_df['check_join'])]
    overview_not_cm.drop(columns = [column for column in (pr_df.columns.intersection(overview_not_cm.columns)) if column not in ['source', 'cm_creative_id']], inplace = True)

    overview_not_cm = overview_not_cm.merge(pr_df[~pr_df.duplicated(['source', 'cm_creative_id'])], on = ['source', 'cm_creative_id'], how = 'left')
    overview_not_cm.loc[:, cm_metrics] = 0

    overview_not_cm['juncao'] = ''
    overview_not_cm.loc[overview_not_cm['impressions'].isnull(), 'juncao'] = 'erro_ga'
    overview_not_cm.loc[~overview_not_cm['impressions'].isnull(), 'juncao'] = 'bm_ausente'

    cm_gaoverview = pd.concat([cm_join_ov, overview_not_cm], axis = 0, ignore_index = True)
    cm_gaoverview.drop(columns = [column for column in events_cm[['totalevents', 'uniqueevents', 'sessionswithevent', 'ga_id']].columns.intersection(cm_gaoverview.columns) if column not in ['ga_id']], inplace = True)
    
    cm_gaevents = events_cm[['eventcategory', 'eventaction', 'eventlabel', 'totalevents', 'uniqueevents', 'sessionswithevent', 'ga_id']].merge(cm_gaoverview[~cm_gaoverview['ga_id'].isnull()], on = 'ga_id', how = 'left')

    cm_gaevents.drop(columns = cm_metrics + ga_metrics, inplace = True)

    return cm_gaoverview, cm_gaevents