import pandas as pd

import os

import datetime

from api.api_facebook import get_facebook
from api.api_googleAds import get_googleAds
from api.api_campaign_manager import get_CampaignManager
from api.api_twitter import get_twitter
from api.api_tiktok import get_tiktok
from api.api_linkedin import get_linkedin

from api.api_googleAnalytics import get_googleAnalytics_bm
from api.api_googleAnalytics import get_googleAnalytics_cm

from functions import *

import warnings
warnings.filterwarnings('ignore')

def main():

    main_dir = os.path.dirname(os.path.abspath(__file__))
    campaign_dir = os.path.abspath(os.path.join(main_dir, '..', 'campanhas'))
    paramet_dir = os.path.abspath(os.path.join(main_dir, '..', 'parametrização'))

    campaigns = pd.read_excel(os.path.abspath(os.path.join(campaign_dir, 'campanhas.xlsx')))
    campaigns.fillna('', inplace = True)

    list_campaings = []

    for _, campaign in campaigns.iterrows():

        is_active = False
        is_updated = False
        have_bm = False
        have_cm = False

        is_facebook = False
        is_google = False
        is_twitter = False
        is_tiktok = False
        is_linkedin = False
        is_analytics = False

        campaign_name = campaign['campanha']
        campaign_path = os.path.abspath(os.path.join(campaign_dir, campaign_name.lower()))

        campaign_param_path = os.path.abspath(os.path.join(paramet_dir, campaign['nome_parametrizacao'] + '.xlsm'))

        if not os.path.isfile(campaign_param_path):
            campaign_param_path = os.path.abspath(os.path.join(paramet_dir, campaign['nome_parametrizacao'] + '.xlsx'))
        
        campaign_start = campaign['data_inicio'].date()
        campaign_end = campaign['data_fim'].date()

        today = datetime.datetime.today().date()
        yesterday = today - datetime.timedelta(days = 1)

        init_date = campaign_start
        end_date = yesterday

        if campaign_start < today <= campaign_end + datetime.timedelta(days = 1):
            is_active = True

            if not os.path.isdir(campaign_path):
                os.mkdir(campaign_path)
                os.mkdir(os.path.join(campaign_path, 'data'))
                os.mkdir(os.path.join(campaign_path, 'source'))
            
            if is_active:
                param = pd.read_excel(campaign_param_path, sheet_name = 'mas_parametrizacao')
                param.drop(columns = ['id_parametro.1', 'term'], inplace = True)
                param.fillna('', inplace = True)

                param.rename(columns = {'Nome do criativo (para programação nas plataformas)': 'ad_name'}, inplace = True)
                
                facebook_accounts = campaign['facebook'].split(',') if campaign['facebook'] else False
                google_accounts = campaign['google'].split(',') if campaign['google'] else False
                tiktok_accounts = campaign['tiktok'].split(',') if campaign['tiktok'] else False
                linkedin_accounts = campaign['linkedin'].split(',') if campaign['linkedin'] else False
                twitter_campaigns = campaign['twitter'].split(',') if campaign['twitter'] else False
                cm_campaigns = campaign['campaign_manager'].split(',') if campaign['campaign_manager'] else False

                view_id = str(int(campaign['ga'])) if campaign['ga'] else ''

                if view_id:
                    is_analytics = True

                print(f'Atualizando campanha: {campaign_name}')

                source_folder = os.path.abspath(os.path.join(campaign_path, 'source'))
                pr = os.path.abspath(os.path.join(source_folder, 'concat_pr.xlsx'))
                gaov = os.path.abspath(os.path.join(source_folder, 'concat_ga.xlsx'))

                if os.path.isfile(gaov):
                    temp_gaov = pd.read_excel(gaov, sheet_name = 'concat_gaov')

                    if temp_gaov.date.max() == yesterday:
                        is_updated = True
                        print(f'{campaign_name} já atualizado!')

                concat_pr = []
                concat_ov = []
                concat_ev = []

                if not is_updated:
                    if facebook_accounts:
                        have_bm = True
                        is_facebook = True

                        facebook = update_bm(campaign_path, init_date, bm = 'Facebook', accounts = facebook_accounts, bm_function = get_facebook)
                        facebook.fillna('', inplace = True)

                        no_content_facebook = facebook[facebook['content'] == '']
                        content_facebook = facebook.drop(facebook[facebook['content'] == ''].index)

                        content_facebook_pr = merge_params(content_facebook, param, campaign_path, 'facebook', content = True)
                        no_content_facebook_pr = merge_params(no_content_facebook, param, campaign_path, 'facebook', content = False)

                        facebook_pr = pd.concat([content_facebook_pr, no_content_facebook_pr], axis = 0, ignore_index = True)
                        facebook_pr.sort_values('date', inplace = True, ignore_index = True)
                        concat_pr.append(facebook_pr)

                    if google_accounts:
                        have_bm = True
                        is_google = True

                        googleAds = update_bm(campaign_path, init_date, bm = 'GoogleAds', accounts = google_accounts, bm_function = get_googleAds)
                        googleAds.fillna('', inplace = True)

                        no_content_googleAds = googleAds[googleAds['content'] == '']
                        content_googleAds = googleAds.drop(googleAds[googleAds['content'] == ''].index)  

                        content_googleAds_pr = merge_params(content_googleAds, param, campaign_path, 'googleAds', content = True)
                        no_content_googleAds_pr = merge_params(no_content_googleAds, param, campaign_path, 'googleAds', content = False)

                        googleAds_pr = pd.concat([content_googleAds_pr, no_content_googleAds_pr], axis = 0, ignore_index = True)
                        googleAds_pr.sort_values('date', inplace = True, ignore_index = True)
                        concat_pr.append(googleAds_pr)

                    if tiktok_accounts:
                        have_bm = True
                        is_tiktok = True

                        tiktok = update_bm(campaign_path, init_date, bm = 'TikTok', accounts = tiktok_accounts, bm_function = get_tiktok)
                        tiktok.fillna('', inplace = True)

                        tiktok_pr = merge_params(tiktok, param, campaign_path, 'TikTok', content = False)

                        tiktok_pr.sort_values('date', inplace = True, ignore_index = True)
                        concat_pr.append(tiktok_pr)

                    if linkedin_accounts:
                        have_bm = True
                        is_linkedin = True

                        linkedin = update_bm(campaign_path, init_date, bm = 'Linkedin', accounts = linkedin_accounts, bm_function = get_linkedin)
                        linkedin.fillna('', inplace = True)

                        linkedin_pr = merge_params(linkedin, param, campaign_path, 'Linkedin', content = False)

                        linkedin_pr.sort_values('date', inplace = True, ignore_index = True)
                        concat_pr.append(linkedin_pr)

                    if twitter_campaigns:
                        have_bm = True
                        is_twitter = True

                        twitter = update_bm(campaign_path, init_date, bm = 'Twitter', campaigns = twitter_campaigns, bm_function = get_twitter)
                        twitter.fillna('', inplace = True)

                        no_content_twitter = twitter[twitter['content'] == '']
                        content_twitter = twitter.drop(twitter[twitter['content'] == ''].index)   
                        
                        content_twitter_pr = merge_params(content_twitter, param, campaign_path, 'twitter', content = True)
                        no_content_twitter_pr = merge_params(no_content_twitter, param, campaign_path, 'twitter', content = False)
                    
                        twitter_pr = pd.concat([content_twitter_pr, no_content_twitter_pr], axis = 0, ignore_index = True)
                        twitter_pr.sort_values('date', inplace = True, ignore_index = True)
                        concat_pr.append(twitter_pr)

                    if cm_campaigns:
                        have_cm = True

                        campaign_manager = update_bm(campaign_path, init_date, bm = 'CampaignManager', campaigns = cm_campaigns, bm_function = get_CampaignManager)
                        campaign_manager_pr = merge_cm_params(campaign_manager, param, campaign_path)

                        campaign_manager_pr.sort_values('date', inplace = True, ignore_index = True)
                        concat_pr.append(campaign_manager_pr)

                    print('Criando base de BM.')
                    concat_pr_all = pd.concat(concat_pr, axis = 0, ignore_index = True)
                    concat_pr_all.to_excel(pr, index = False, sheet_name = 'concat_pr')

                    if have_bm and is_analytics:
                        bm_campaigns = []
                        bm_campaigns += list(googleAds.campaign.str.replace('  ', ' ').unique())
                        bm_campaigns += list(facebook.campaign.unique())

                        bm_overview, bm_events = update_ga(campaign_path, view_id, init_date, ga = 'bm', campaigns = bm_campaigns, ga_function = get_googleAnalytics_bm)

                        if is_facebook:
                            facebook_ov, facebook_ev = merge_ga(facebook_pr, bm_overview, bm_events, bm = 'facebook')
                            concat_ov.append(facebook_ov)
                            concat_ev.append(facebook_ev)
                            
                        if is_google:
                            googleAds_ov, googleAds_ev = merge_ga(googleAds_pr, bm_overview, bm_events, bm = 'google')
                            concat_ov.append(googleAds_ov)
                            concat_ev.append(googleAds_ev)
                        
                        if is_tiktok:
                            tiktok_ov, tiktok_ev = merge_ga(tiktok_pr, bm_overview, bm_events, bm = 'tiktok')
                            concat_ov.append(tiktok_ov)
                            concat_ev.append(tiktok_ev)

                        if is_linkedin:
                            linkedin_ov, linkedin_ev = merge_ga(linkedin_pr, bm_overview, bm_events, bm = 'linkedin')
                            concat_ov.append(linkedin_ov)
                            concat_ev.append(linkedin_ev)

                        if is_twitter:
                            twitter_ov, twitter_ev = merge_ga(twitter_pr, bm_overview, bm_events, bm = 'twitter')
                            concat_ov.append(twitter_ov)
                            concat_ev.append(twitter_ev)

                    if have_cm and is_analytics:
                        cm_overview, cm_events = update_ga(campaign_path, view_id, init_date, ga = 'cm', campaigns = cm_campaigns, ga_function = get_googleAnalytics_cm)
                        campaign_manager_ov, campaign_manager_ev = merge_ga_cm(campaign_manager_pr, cm_overview, cm_events)
                        concat_ov.append(campaign_manager_ov)
                        concat_ev.append(campaign_manager_ev)
                    
                    if is_analytics:
                        concat_gaov = pd.concat(concat_ov, axis = 0, ignore_index = True)
                        concat_gaev = pd.concat(concat_ev, axis = 0, ignore_index = True)

                        print('Criando base de GA.')

                        if campaign_name == 'Cultura' or campaign_name == 'Circuito Agro':
                            concat_gaov.to_csv(os.path.abspath(os.path.join(source_folder, 'concat_ga.csv')), encoding = 'utf-8', index = False)

                        else:
                            writer = pd.ExcelWriter(gaov)
                            concat_gaov.to_excel(writer, index = False, sheet_name = 'concat_gaov')
                            concat_gaev.to_excel(writer, index = False, sheet_name = 'concat_gaev')
                            writer.save()

                        
                    if campaign_name == 'Soluções Digitais':
                        adjust = update_adjust(campaign_path, init_date)

                        adjust_os = os.path.abspath(os.path.join(source_folder, 'adjust_prep.xlsx'))
                        adjust.to_excel(adjust_os, index = False, sheet_name = 'adjust_prep')

            print(20*'-')
    return


if __name__ == '__main__':
    main()