import pandas as pd
import requests
import json
import re
from urllib.parse import parse_qs, urlencode, urlparse


FACEBOOK_GRAPH_URL = "https://graph.facebook.com/"
VALID_API_VERSIONS = ["7.0", "8.0"]

class GraphAPI:
    """A client for the Facebook Graph API.

    https://developers.facebook.com/docs/graph-api

    """

    def __init__(
        self, 
        page_id=None, 
        access_token=None, 
        version=None):
        self.session = requests.Session()

        if page_id:

            self.page_id = page_id
        else :
              raise GraphAPIError(
                    "Facebook Page ID is missing"
                )
        
        if access_token:

            self.access_token = access_token
        else:
            raise GraphAPIError(
                    "Facebook Page access token is missing"
                )

        # The default version is only used if the version kwarg does not exist.
        default_version = VALID_API_VERSIONS[0]

        if version:
            version_regex = re.compile(r"^\d\.\d{1,2}$")
            match = version_regex.search(str(version))
            if match is not None:
                if str(version) not in VALID_API_VERSIONS:
                    raise GraphAPIError(
                        "Valid API versions are "
                        + str(VALID_API_VERSIONS).strip("[]")
                    )
                else:
                    self.version = "v" + str(version)
            else:
                raise GraphAPIError(
                    "Version number should be in the"
                    " following format: #.# (e.g. 2.0)."
                )
        else:
            self.version = "v" + default_version


    


    def get_connections(self, connection_name, **args):
        """Fetches the connections for given object."""
        return self.request(
            "{0}/{1}/{2}".format(self.version, self.page_id, connection_name), args
        )

    def get_all_connections(self, connection_name, **args):
        """Get all pages from a get_connections call

        This will iterate over all pages returned by a get_connections call
        and yield the individual items.
        """
        data = []
        while True:
            page = self.get_connections(connection_name = connection_name, **args)
            for single_item in page["data"]:
                data.append(single_item)   
            next = page.get("paging", {}).get("next")
            if not next:
                return data
            args = parse_qs(urlparse(next).query)
            
            

    def fans(self, since):
        
        number_of_fans = self.get_connections(connection_name='insights',
                                         metric='page_fans',
                                         period='day',
                                         since=since,
                                         access_token=self.access_token)

        fans_count_raw = json.dumps(number_of_fans)
        fans_count_json = json.loads(fans_count_raw)
        fans_list = fans_count_json['data'][0]['values']
        return fans_list

    def fans_per_city(self, since):
        fans_city = self.get_connections(connection_name='insights',
                                         metric='page_fans_city',
                                         period='day',
                                         since=since,
                                         access_token=self.access_token)
        fans_city_raw = json.dumps(fans_city)
        fans_city_json = json.loads(fans_city_raw)
        fans_city_list = fans_city_json['data'][0]['values']                                 
        return fans_city_list

    def fans_growth_day(self, since):
        fans_growth = self.get_connections(connection_name='insights',
                                         metric='page_fan_adds',
                                         period='day',
                                         since=since,
                                         access_token=self.access_token)
        fans_growth_raw = json.dumps(fans_growth)
        fans_growth_json = json.loads(fans_growth_raw)
        fans_growth_list = fans_growth_json['data'][0]['values']                                 
        return fans_growth_list
                                    

    def fans_gender_age(self, since):

        ## gender age possible mapping
        gender_age = ['F.13-17','F.18-24','F.25-34','F.35-44','F.45-54','F.55-64','F.65+',
        'M.13-17','M.18-24','M.25-34','M.35-44','M.45-54','M.55-64','M.65+']
        
        fans_gender_age = self.get_connections(connection_name='insights',
                                         metric='page_fans_gender_age',
                                         period='day',
                                         since=since,
                                         access_token=self.access_token)
        fans_gender_age_raw = json.dumps(fans_gender_age)
        fans_gender_age_json = json.loads(fans_gender_age_raw)
        fans_gender_age_list = fans_gender_age_json['data'][0]['values']
        for item in fans_gender_age_list:
            for gender_age_item in gender_age:
                if gender_age_item in item['value']:
                    pass
                else:
                    item['value'].update({gender_age_item:0})
        return fans_gender_age_list

    def post_engagement(self, since) :
        engagement = self.get_connections(connection_name='insights',
                                                    metric='page_post_engagements',
                                                    period='day',
                                                    since=since,
                                                    access_token=self.access_token)
        

        engagement_count_raw = json.dumps(engagement)
        engagement_count_json = json.loads(engagement_count_raw)
        engagement_list = engagement_count_json['data'][0]['values']
        return engagement_list

    def posts(self,since) : 
        posts = self.get_all_connections(connection_name="posts", 
        fields=str("created_time,attachments,shares,reactions.limit(0).summary(1),reactions.type(LIKE).limit(0).summary(1).as(like),reactions.type(LOVE).limit(0).summary(1).as(love),reactions.type(HAHA).limit(0).summary(1).as(haha),reactions.type(WOW).limit(0).summary(1).as(wow),reactions.type(SAD).limit(0).summary(1).as(sad),reactions.type(ANGRY).limit(0).summary(1).as(angry),comments.limit(0).filter(toplevel).total_count(1).summary(1)"),
        since=since,
        access_token=self.access_token)
        posts_raw = json.dumps(posts)
        posts_json = json.loads(posts_raw)

        posts_df = pd.json_normalize(posts_json)


        not_attachments = posts_df[posts_df['attachments.data'].isna()]
        attachments = posts_df[posts_df['attachments.data'].notna()]


        posts_df = (pd.concat({i: pd.json_normalize(x) for i, x in attachments.pop('attachments.data').items()})
            .reset_index(level=1, drop=True)
            .join(attachments)
            .reset_index(drop=True))

        posts_df = pd.concat([posts_df,not_attachments])

        
        posts_df['created_time'] =  pd.to_datetime(posts_df['created_time'], format='%Y-%m-%d')
        posts_df['created_date'] =  posts_df['created_time'].dt.strftime('%Y-%m-%d')
        posts_df['created_time'] = posts_df['created_time'].astype(int)/ 10**9
        posts_df['page_id'] = self.page_id
        posts_df = posts_df.fillna(False)

        

        out = posts_df[['page_id','id','title','type','url','description',
        'created_time','created_date', 'shares.count','reactions.summary.total_count',
        'like.summary.total_count','love.summary.total_count','haha.summary.total_count',
        'wow.summary.total_count','sad.summary.total_count','angry.summary.total_count',
        'comments.summary.total_count']]

        return out


    def request(
        self, path, args=None, method=None
    ):
        """Fetches the given path in the Graph API.

        We translate args to a valid query string. .

        """
        if args is None:
            args = dict()
        
        try:
            response = self.session.request(
                method or "GET",
                FACEBOOK_GRAPH_URL + path,
                params=args ,
                
            )
        except requests.HTTPError as e:
            response = json.loads(e.read())
            raise GraphAPIError(response)

        headers = response.headers
        if "json" in headers["content-type"]:
            result = response.json()
        else:
            raise GraphAPIError("Maintype was not text or querystring")

        if result and isinstance(result, dict) and result.get("error"):
            raise GraphAPIError(result)
        return result

    

class GraphAPIError(Exception):
    def __init__(self, result):
        self.result = result
        self.code = None
        self.error_subcode = None

        try:
            self.type = result["error_code"]
        except (KeyError, TypeError):
            self.type = ""

        # OAuth 2.0 Draft 10
        try:
            self.message = result["error_description"]
        except (KeyError, TypeError):
            # OAuth 2.0 Draft 00
            try:
                self.message = result["error"]["message"]
                self.code = result["error"].get("code")
                self.error_subcode = result["error"].get("error_subcode")
                if not self.type:
                    self.type = result["error"].get("type", "")
            except (KeyError, TypeError):
                # REST server style
                try:
                    self.message = result["error_msg"]
                except (KeyError, TypeError):
                    self.message = result

        Exception.__init__(self, self.message)