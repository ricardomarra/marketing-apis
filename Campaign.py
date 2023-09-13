from api.api_facebook import get_facebook_data
from api.api_googleAds import get_googleAds_data
from api.api_campaign_manager import get_cm_data
from api.api_twitter import get_twitter_data

from api.api_googleAnalytics import get_googleAnalytics_bm_data
from api.api_googleAnalytics import get_googleAnalytics_cm_data


class Campaign:

    def __init__(self, name: str, start_date: str, end_date: str, campaign_path: str):
        self.name = name
        self.start_date = start_date
        self.end_date = end_date
        self.campaign_path = campaign_path
        self.is_active = False
        self.facebook = None
        self.googleAds = None
        self.twitter = None
        self.campaign_manager = None
        self.bm_googleAnalytics = None
        self.cm_googleAnalytics = None

    def get_facebook(self, accounts, start_date, end_date):
        self.facebook = get_facebook_data(accounts, start_date, end_date)

    def get_googleAds(self, accounts, start_date, end_date):
        self.googleAds = get_googleAds_data(accounts, start_date, end_date)

    def get_twitter(self, campaigns, start_date, end_date):
        self.twitter = get_twitter_data(self.campaign_path, campaigns, start_date, end_date)

    def get_campaign_manager(self, campaigns, start_date, end_date):
        self.campaign_manager = get_cm_data(self.campaign_path, campaigns, start_date, end_date)
        
    def get_bm_googleAnalytics(self, view, campaigns, start_date, end_date):
        self.bm_googleAnalytics = get_googleAnalytics_bm_data(self.campaign_path, view, campaigns, start_date, end_date)
    
    def get_cm_googleAnalytics(self, view, campaigns, start_date, end_date):
        self.cm_googleAnalytics = get_googleAnalytics_cm_data(self.campaign_path, view, campaigns, start_date, end_date)

# def main():

#     campaign = Campaign('teste', '0', '0', pd.DataFrame([]))
#     campaign.get_facebook_data()

#     print(campaign.facebook.head())

# if __name__ == '__main__':
#     main()