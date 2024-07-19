#%%-------------------------------------------------------------------------------------
#                               LinkedIn API Connector
#                    Purpose: This script connects to the LinkedIn API to retrieve
#                    data such as, post performance etc
#---------------------------------------------------------------------------------------
from datetime import datetime,timedelta
import time
from pendulum import timezone
import numpy as np
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import random
from tqdm.auto import tqdm
from veetility import utility_functions
from veetility import snowflake as sf
# from veetility import generic_functions as gf
import re
from collections import Counter
import json
load_dotenv()


util = utility_functions.UtilityFunctions('LinkedIn API', db_user=os.getenv('db_user'),
                                           db_password=os.getenv('db_password'), db_host=os.getenv('db_host'),
                                           db_port=os.getenv('db_port'), db_name=os.getenv('db_name'), log_name='Main')

#%%
class LinkedInAPI:
    """A class to interact with LinkedIn's API for various data fetching and transformation tasks.

    This class provides methods to authenticate a user, fetch organization IDs, fetch posts, 
    fetch stats for posts, and convert time formats among other tasks. The class is designed 
    to be a comprehensive utility for fetching and analyzing LinkedIn organizational data.

    The main statistics (impressions, cicks, reach, likes, shares and comments) are fetched with the fetch_stats_for_a_post()
    method. 
    
    To get more detailed reactions such as Empathy, Praise, Interest, Appreciation or Funny, the fetch_reactions_for_a_post() has to be called.

    To get video views call the method fetch_video_views_for_a_post().

    Attributes:
        headers_v1 (dict): Headers for API requests that use LinkedIn's API v1.
        headers_v2 (dict): Headers for API requests that use LinkedIn's API v2.
        org_ids (list): List of organization IDs associated with the authenticated user.
        posts_df (DataFrame): DataFrame containing fetched posts data.
        post_stats_dict (dict): Dictionary containing statistics for fetched posts.

    Examples:
        api_client = LinkedIn(api_token="your-api-token")
        api_client.fetch_org_ids()
        >>> api_client.fetch_posts()

    Notes:
        1. The class is designed to handle both individual and batch requests for efficiency.
        2. Ensure that you have the required libraries installed and valid API credentials."""

    def __init__(self, api_token=None,time_zone="Europe/London"):
        """Initialize the LinkedIn API client with an API from the developer portal.
            Developer portal: https://developer.linkedin.com/
            
            Args:
                api_token (str, optional): The API token for authentication. 
                    If not provided, a ValueError will be raised.
                time_zone (str, optional): The time zone for any date-time 
                    manipulations. Defaults to "Europe/London".
            
            Raises:
                ValueError: If the `api_token` is not provided.
                
            Attributes:
                api_token (str): Stores the API token for use in other methods.
                headers_v1 (dict): Stor=es headers for API X-Restli-Protocol-Version 1.
                headers_v2 (dict): Stores headers for API VX-Restli-Protocol-Version 2.
                time_zone (timezone): Stores the time zone information."""
        
        if api_token is None:
            raise ValueError("API token must be provided")
        self.api_token = api_token

        self.headers_v1 = {
            'X-Restli-Protocol-Version': '1.0.0', #Version 2 doesn't work for individual posts...
            'LinkedIn-Version' : '202304',
            'Authorization': 'Bearer ' + self.api_token,
            'Content-Type': 'application/json'
            } 
        
        self.headers_v2 = {
            # 'X-Restli-Protocol-Version': '2.0.0',
            'LinkedIn-Version' : '202308',
            'Authorization': 'Bearer ' + self.api_token
            # 'Content-Type': 'application/json'
            } 
        
        self.time_zone = timezone(time_zone)
        self.retry_count = 0
    
    def exponential_backoff_delay(self, retry_count):
        """Implements an exponential backoff delay with jitter for server response problems.
            
            Args:
                retry_count (int): The number of times the request has been retried.
                
            Side Effects:
                Introduces a time delay into the program execution.
                
            Notes:
                The delay is calculated as follows:
                    delay = (4 ** retry_count + 1) * 5
                A jitter (random noise) is also added to the delay."""
        
        delay = (4 ** retry_count + 1) * 5 # Exponential backoff
        jitter = random.uniform(0, 0.1 * delay)  # Adding jitter
        print(f"Starting exponential delay of {round(delay + jitter,2)} seconds to give server time to recover")
        time.sleep(delay + jitter)
    
    def run_request_with_error_handling(self, 
                                        url, 
                                        headers, 
                                        params=None, 
                                        max_retries=5, 
                                        expected_json_response=True, 
                                        ):
        """Wrapper function to execute an HTTP GET request with error handling and mutliple time delayed retries.
            
        Args:
            url (str): The URL endpoint to call.
            headers (dict): The headers to include in the request.
            params (dict, optional): The parameters to include in the request. Defaults to None.
            max_retries (int, optional): The maximum number of times to retry the request. Defaults to 5.

        Returns:
            requests.Response: The response object if the request is successful.

        Raises:
            HTTPError: If a permanent HTTP error occurs (status code 400, 401, 403).
            Exception: If an unexpected error occurs or if the request fails multiple times."""

        try:
            if params == None:
                response = requests.get(url=url, headers=headers)
                #print(response.headers.get('X-LI-UUID'))
            else:
                response = requests.get(url=url, headers=headers, params=params)
                
            self.response = response
            response.raise_for_status()

            rate_limit_info = {
                'Limit': response.headers.get('RateLimit-Limit'),
                'Remaining': response.headers.get('RateLimit-Remaining'),
                'Reset': response.headers.get('RateLimit-Reset'),
            }

            # print(f"{rate_limit_info}")

            # Sometimes even though the status code is 200, the response object is actually a None value, that doesn't get picked up 
            # by testing for response == None, weird behaviour I couldn't figure out. So below i test to see if getting the json from response is possible
            # else it will break the try loop and try again after an exponential delay
            if expected_json_response:
                response.json() 
            if response.status_code == 200:
                self.retry_count=0
                return response
            else:
                raise Exception(f"Raise for status didn't work, status code = {response.status_code}")


        except requests.exceptions.HTTPError as errh:
        
            if response.status_code == 429:
                print(f"This is a Rate limit error code :{response.status_code}: message: {response.json()['message']}\
                            Request params are: {params}. Visit https://learn.microsoft.com/en-us/linkedin/shared/api-guide/concepts/rate-limits for more info" )
                self.exponential_backoff_delay(self.retry_count)

            elif response.status_code in [400, 401, 403]:
                print(f"Permanent HTTP Error: {response.status_code}, message: {response.json().get('message', '')}, Request params are: {params}.")
                raise Exception(f"Permanent HTTP Error: {response.status_code}, message: {response.json().get('message', '')}, Request params are: {params}.")

            elif response.status_code in [429, 500, 502, 503, 504]:
                print(response.json())
                print(f'headers = {response.headers}')
                print(f"Temporary HTTP Error: {response.status_code}, message: {response.json().get('message', '')}, Request params are: {params}.")
                self.exponential_backoff_delay(self.retry_count)  # Or follow the Retry-After header if available
            
            # Other HTTP issues issues
            else:
                print(f"Unexpected HTTP Error: {response.status_code}, message: {response.json().get('message', '')}, Request params are: {params}.")
                
        except Exception as e:
            # Catch all other exceptions
            print(f"An unexpected error occurred: {e}, Request params are: {params}. url = {url}")
            self.exponential_backoff_delay(self.retry_count)

        # Increment and check retries for potentially temporary issues
        self.retry_count += 1
        if self.retry_count <= max_retries:
            return self.run_request_with_error_handling(url, headers, params)
        else:
            raise('Request failed multiple times - Check Logs')
            
    
    def convert_unix_datetime(self, unix_format):
        """Converts a Unix timestamp to a pandas NaT (Not-a-Time) or datetime64 object.
            
        Args:
            unix_format (int, str, or None): The Unix timestamp to convert. Can be None, np.nan, an integer, or a string that can be converted to an integer.
            
        Returns:
            pandas.Timestamp or pandas.NaT: A pandas datetime64 object representing the converted Unix timestamp. Returns pandas.NaT if the conversion fails or input is invalid.
            
        Notes:
            1. Unix timestamps are expected to be in milliseconds.
            2. If the provided timestamp is a string and not a digit, pandas.NaT is returned.
            3. If an exception occurs during conversion, pandas.NaT is returned."""
        
        if unix_format is None or unix_format is np.nan:
            print("Nan or None values")
            return pd.NaT
        
        if isinstance(unix_format, str):
            if unix_format.isdigit():
                unix_format = int(unix_format)
            else:
                print("The input is a string that is not a number")
                pd.NaT
        
        try: 
            unix_format = int(unix_format)
        except (ValueError, TypeError):
            return pd.NaT
        
        try:
            unix_format_secs = unix_format / 1000
            return pd.to_datetime(datetime.fromtimestamp(unix_format_secs))
        except Exception as e:
            print(f"An Error occured {e}")
            return pd.NaT
    
    def convert_pd_datetime_to_unix(self, pd_datetime):
        """Converts a pandas Timestamp to a Unix timestamp in milliseconds.

        The method first converts the Timestamp to a Unix timestamp in seconds, then multiplies it by 1000 to get the timestamp in milliseconds.
        
        Args:
            pd_datetime (pandas.Timestamp): A pandas Timestamp object to convert.
            
        Returns:
            int or None: An integer representing the Unix timestamp in milliseconds if the conversion is successful. Returns None if an error occurs."""

        try:
            # Convert pd.Timestamp to Unix timestamp in seconds
            unix_timestamp = pd_datetime.timestamp()
            
            # Convert it to milliseconds
            unix_timestamp_ms = int(unix_timestamp * 1000)
            
            return unix_timestamp_ms
        
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
    
    def fetch_org_ids(self):
        """Fetches the organization IDs associated with the authenticated LinkedIn user who has the role of ADMINISTRATOR.
        This is useful if you want a list of organisation ids to iterate through to fetch information for each client.
        
        Side Effects:
            Populates the `org_ids` attribute of the class instance with a list of organization IDs, if any are found.
            Prints a message to the console if no data is found in the API response."""
        
        url = "https://api.linkedin.com/rest/organizationAcls?q=roleAssignee"

        response = self.run_request_with_error_handling(url, self.headers_v2)

        org_ids = [x.get('organization').replace("urn:li:organization:", "") for x in response.json().get("elements")]
        if org_ids:
            self.org_ids = org_ids
        else:
            print("No data found in API response.")

    
    def fetch_org_info(self):
        """Fetches the organization information for the authenticated LinkedIn user based on organization IDs.
    
        Returns:
            pandas.DataFrame: A DataFrame containing the organization information. Each row corresponds to an organization.
            
        Side Effects:
            1. Calls `fetch_org_ids` method if `org_ids` attribute is not already set.
            2. Prints the API response to the console."""
        
        org_info_df = pd.DataFrame()
        headers = self.headers_v2

        if not hasattr(self, 'org_ids'):
            self.fetch_org_ids()

        for org_id in self.org_ids:
            url = f"https://api.linkedin.com/rest/organizations{org_id}"
            params={'ids' : str(org_id)}

            response = self.run_request_with_error_handling(url, headers, params=params)
            try:
                df_nested_list = response.json()
                df_unnested = pd.json_normalize(df_nested_list['results'][org_id], max_level=0)
                data = [org_info_df,df_unnested]
                org_info_df = pd.concat(data)
            except:
                pass
        
        self.org_info_df = org_info_df

        # Sometimes an account can appear in the organisational entity control list but not in the organisational info endpoint
        # This may be because a previous account we worked with no longer gives us permission.
        # Therefore update the org ids to represent the info from the org info endpoint. This will ensure post metrics can be extracted 
        
        # if self.org_ids != self.org_info_df.id.unique().tolist():
        #     gf.slack_error_notification(webhook=Variable.get("slack_webhook"),)

        return org_info_df
    
    def fetch_posts(self,org_ids=None):
        """Fetches LinkedIn posts associated with the specified organization IDs or those found by the fetch_org_ids method.
            The method utilizes `convert_unix_datetime` to convert Unix timestamps in the columns 'createdAt', 'lastModifiedAt' and
            'publishedAt'] to datetime objects.
    
        Args:
            org_ids (list, optional): List of organization IDs for which to fetch posts. Defaults to None, if None it will
                                     fetch the list of org_ids associated with the linkedIn account using the fetch_org_ids() function

        Returns:
            pandas.DataFrame: A DataFrame containing fetched posts. Each row corresponds to a post."""
        posts_df = pd.DataFrame()
        retry_count = 0
        
        if org_ids == None:
            self.fetch_org_info()

            self.org_ids == self.org_info_df.id.unique().tolist()

            org_ids = self.org_ids
        
        for org_id in org_ids:
            print(f'org_id = {org_id}')
            start = 0
            #first just fetch the first page to find out the total number of posts there are
            URL = "https://api.linkedin.com/rest/posts?q=author&author=urn%3Ali%3Aorganization%3A" +""+ str(org_id) +"" + "&count=1"
            
            response = self.run_request_with_error_handling(URL, self.headers_v2)

            num_posts = response.json()['paging']['total']
            num_iterations = int(num_posts / 100)  # Assuming pages are fetched 100 at a time

            with tqdm(total=num_iterations, desc=f"Fetching for org {org_id}") as pbar:
                for start in range(0, num_posts, 100):
                    url = f"https://api.linkedin.com/rest/posts?q=author&author=urn%3Ali%3Aorganization%3A{org_id}&count=100&start={start}"
                    response = self.run_request_with_error_handling(url, self.headers_v2)
                    try:
                        df_nested_list = response.json()
                    except:
                        print(f"response returned {response}")
                        retry_count +=1
                        if retry_count <=5:
                            self.exponential_backoff_delay(retry_count)
                            response = self.run_request_with_error_handling(url, self.headers_v2)
                            df_nested_list = response.json()

                    df_unnested = pd.json_normalize(df_nested_list['elements'], max_level=5)
                    data = [posts_df, df_unnested]
                    posts_df = pd.concat(data)

                    pbar.update(1)  # Increment the progress bar
        
        for col in ['createdAt', 'lastModifiedAt', 'publishedAt']: #Date columns which ar ein unix time format
            posts_df[col] = posts_df[col].apply(lambda x: self.convert_unix_datetime(x))
        
        posts_df.columns = [x.replace('.','_') for x in posts_df.columns] #Some of the column names have "." seperators which doesn't work with Snowflake 
        #The object and date columns sometimes can be a mix of strings and other data types which snowflake doesn't like, so convert them to strings
        # object_columns = posts_df.select_dtypes(include=['object']).columns
        # posts_df[object_columns] = posts_df[object_columns].astype(str)

        self.posts_df = posts_df

        return posts_df
    
    def fetch_stats_for_a_post(self, post_id, org_id):
        """Fetches impressions, cicks, reach, likes, shares and comments for a specific LinkedIn post associated with a given organization.
        The org_id is requried for this endpoint.

        Args:
            post_id (str or int): The identifier for the post for which to fetch statistics.
            org_id (str or int): The identifier for the organization under which the post was created.

        Returns:
            dict: A dictionary containing the fetched post statistics. 

        Notes:
            1. This method uses LinkedIn v1 API and requires appropriate headers for authentication.
            2. The method relies on `run_request_with_error_handling` for API requests and error handling.."""
        post_id = str(post_id).replace(':','%3A')

        org_id_modified = str(org_id).replace(':','%3A') #Sometimes the endpoint wants colons replace with %3A, not sure why

        if 'share' in post_id:
            post_type_q = 'shares[0]'
        
        if 'ugcPost' in post_id:
            post_type_q = 'ugcPosts[0]'

        url = f'https://api.linkedin.com/rest/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={org_id_modified}&{post_type_q}={post_id}'
        
        print(f"url = {url}")

        response = self.run_request_with_error_handling(url, self.headers_v2, params=None)
        
        return response.json()

    def fetch_stats_for_posts(self, posts_df=None, post_id_col='id',org_id_col='author'):
        """Fetches statistics for a list of LinkedIn posts based on their IDs and associated organization IDs.
            The function adds a new column 'date_fetched_from_api' to the DataFrame with the current date which can be used
            to track how a posts metrics change over time.

            Args:
                posts_df (pd.DataFrame, optional): DataFrame containing information about the posts for which to fetch statistics.
                    If None, the method will use self.posts_df that would be created by previously running fetch_posts().
                post_id_col (str, optional): The column name in `posts_df` that contains the post IDs. Defaults to 'id'.
                org_id_col (str, optional): The column name in `posts_df` that contains the organization IDs. Defaults to 'author'.

            Returns:
                pd.DataFrame: A DataFrame containing both the original post information and the fetched statistics.
                
            Notes:
                1. The function fetches statistics using `fetch_stats_for_a_post` method.
                2. If fetching fails, an Excel sheet "Failed_fetch_posts_stats.xlsx" will be generated, so that data can be used to backfill if a large amount
                    of post data was fetched before it broke. You could then only fetch the stats for the remaining posts to save time."""
        
        if posts_df is None:
            posts_df = self.posts_df

        orgs_and_posts = list(set(list(zip(posts_df[org_id_col],posts_df[post_id_col]))))
        self.post_stats_dict = {}

        for org_and_post in tqdm(orgs_and_posts):
            try:
                stats = self.fetch_stats_for_a_post(org_and_post[1], org_and_post[0])
            except:
                print(f"Fetching stats for post failed: {org_and_post[1]} org_id = {org_and_post[0]} ")
                #if the process fails save an excel sheet of where we got to to save fetching everything again
                pd.DataFrame(self.post_stats_dict).transpose().reset_index().to_excel("Failed_fetch_posts_stats.xlsx")

            if stats == 'Request Unsuccessful - Check Logs':
                print('Request Unsuccessful - Check Logs')

            elif stats['elements']:
                self.post_stats_dict[org_and_post[1]] = stats['elements'][0]['totalShareStatistics']
                #print(post_stats_dict[org_and_post[1]])

        posts_stats = pd.DataFrame(self.post_stats_dict).transpose().reset_index()
        posts_stats = posts_stats.rename(columns={'index':post_id_col})

        posts_df_with_stats = pd.merge(posts_df, posts_stats, left_on=post_id_col, right_on=post_id_col, how='left')

        posts_df_with_stats['date_fetched_from_api'] = pd.to_datetime(datetime.today())

        return posts_df_with_stats  
    
    def fetch_video_views_for_a_post(self, post_id):
        """Fetches video view statistics for a specific LinkedIn post based on its ID.

        Args:
            post_id (str or int): The unique identifier for the LinkedIn post for which to fetch video view statistics.

        Returns:
            dict: A dictionary containing the JSON response from the LinkedIn API, which includes video view statistics.

        Notes:
            1. The function uses the `run_request_with_error_handling` method for making the API call.
            2. The 'VIDEO_VIEW' type is hardcoded as it's specific to fetching video views.

        Raises:
            Any exceptions raised by `run_request_with_error_handling` will be propagated."""
        post_id = str(post_id)
        params = {
            'q' : 'entity',
            'entity' : f'{post_id}',
            'type' : 'VIDEO_VIEW'
        }
        url = 'https://api.linkedin.com/rest/videoAnalytics'
        response = self.run_request_with_error_handling(url, self.headers_v2, params)
        return response.json()
    
    def fetch_video_views_for_multiple_posts(self, posts_df, media_type_col, post_id_col='id',output_col='videoViews'):
        """Fetches video view statistics for multiple LinkedIn posts and adds them to a DataFrame.

        Args:
            posts_df (pd.DataFrame): A DataFrame containing information about multiple LinkedIn posts. It must include the columns specified by `media_type_col` and `post_id_col`.
            media_type_col (str): The column name in `posts_df` that contains the media type information for each post.
            post_id_col (str, optional): The column name in `posts_df` that contains the post IDs. Defaults to 'id'.
            output_col (str, optional): The column name in which to store the fetched video views. Defaults to 'videoViews'.

        Returns:
            pd.DataFrame: A DataFrame with an additional column containing video view statistics, specified by `output_col`.

        Notes:
            1. The function identifies posts with videos by checking if the string 'video' appears in the `media_type_col` value for each row.
            2. If the API call fails or if the post does not contain a video, np.nan will be inserted for that post.
        Raises:
            Any exceptions raised by `fetch_video_views_for_a_post` will be caught and will result in np.nan being inserted."""
            
        posts_df[media_type_col] = posts_df[media_type_col].astype(str)

        def video_views_wrapper(post_id, media_type_string):
            if 'video' in media_type_string:
                try:
                    return self.fetch_video_views_for_a_post(post_id)['elements'][0]['value']
                except:
                    return np.nan
            else:
                return np.nan

        posts_df[output_col] = posts_df.apply(lambda x: video_views_wrapper(x[post_id_col], x[media_type_col]), axis=1)

        return posts_df

    
    def fetch_reactions_for_a_post(self, urn_id, community_api_token , count=100):
        """Fetches the count of different types of reactions for a LinkedIn post specified by its URN (Uniform Resource Name) ID.

        Args:
            urn_id (str): The URN ID of the LinkedIn post for which reactions are to be fetched. Can be either 'ugcPost' or 'share'.
            count (int, optional): The number of reactions to fetch per API call. Defaults to 100.

        Returns:
            collections.Counter: A Counter object with keys as reaction types and values as their respective counts.

        Notes:
            1. This method internally uses pagination to fetch all reactions in batches of size `count`.
            2. The function includes an exponential backoff delay mechanism to handle failed requests and retries up to 5 times.
            3. The URN ID is URL-encoded internally to handle special characters.

        Raises:
            Exceptions are caught internally, logged, and retried up to 5 times.
            If fetching fails after 5 retries, the loop terminates."""
        
        urn_id = str(urn_id).replace(':','%3A') #Can be a ogcPost or a share
        count = 100
        start = 0
        params = {
            'q' : 'entity',
            'start': start,
            'count': count
        }
        headers = self.headers_v2
        headers['Authorization'] = 'Bearer ' + community_api_token
        headers['X-Restli-Protocol-Version'] = '2.0.0'

        reactions_count = Counter()

        #Initial query to find total number of reactions
        url = f'https://api.linkedin.com/rest/reactions/(entity:{urn_id})'
        response = self.run_request_with_error_handling(url, headers, params)
        total_reactions = response.json()['paging']['total']
        num_iterations = int(total_reactions/count)
        
        with tqdm(total=num_iterations, desc=f"Fetching for {urn_id}") as pbar:
            for start in range(0, total_reactions, 100):
                params['start'] = start

                url = f'https://api.linkedin.com/rest/reactions/(entity:{urn_id})'
                response = self.run_request_with_error_handling(url, headers, params)
                try:
                    elements = response.json()['elements']
                except:
                    retry_count = 0
                    
                    retry_count +=1
                    if retry_count <=5:
                        self.exponential_backoff_delay(retry_count)
                        response = self.run_request_with_error_handling(url, headers, params)
                        elements = response.json['elements'] #Each of the json elements describing who reacted and what reaction
                
                reactions_count = reactions_count + Counter([item['reactionType'] for item in elements])
                pbar.update(1)
        
        return reactions_count
    
    def fetch_reactions_for_multiple_posts(self, list_of_posts, community_api_token, id_col_name='id'):
        """Fetches the count of different types of reactions for a list of LinkedIn posts and aggregates them into a DataFrame.

        Args:
            list_of_posts (list): A list of URN IDs for the LinkedIn posts for which reactions are to be fetched.
            id_col_name (str, optional): The name of the column in the output DataFrame that will store the URN IDs. Defaults to 'id'.

        Returns:
            pandas.DataFrame: A DataFrame where each row represents the reaction count for a specific post. Columns represent different types of reactions, and the index is reset with a column containing URN IDs.

        Notes:
            1. Calls the `fetch_reactions_for_a_post` method internally for each post in `list_of_posts`.
            2. The DataFrame's NaN values are replaced with 0.
            3. The DataFrame's index is reset, and the column for URN IDs is renamed according to `id_col_name`."""

        reactions_df = pd.DataFrame()
        print(f"Number of posts = {len(list_of_posts)}")
        for post in list_of_posts:
            reactions = self.fetch_reactions_for_a_post(post, community_api_token)
            reactions_df = pd.concat([reactions_df,pd.DataFrame(reactions,index=[post])])
        
        reactions_df.fillna(0, inplace=True)
        reactions_df.reset_index(inplace=True)
        reactions_df.rename(columns={'index':id_col_name},inplace=True)
        return reactions_df

    def fetch_follower_count(self, dict_of_urls):
        """Fetches the follower count of companies from their LinkedIn URLs.

        This function sends a GET request to each URL present in the given dictionary, scrapes the page's HTML to find the follower count, and then stores this information in a pandas DataFrame.
        """
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}
        
        output_dict = {}
        
        for company, url in dict_of_urls.items():
            response = self.run_request_with_error_handling(url, headers=headers, expected_json_response=False)
            html = response.text
            print(html)
            pattern = r'([\d,]+) followers on LinkedIn'
            match = re.search(pattern, html)
            try:
                # Extract the follower count, remove commas, and convert to integer
                follower_count = int(match.group(1).replace(',', ''))
                output_dict[company] = follower_count
            except:
                print("No follower count found")
                output_dict[company] = 'Error'
        print(f"output_dict= {output_dict}")
        output_df = pd.DataFrame(output_dict.values(), index=output_dict.keys(), columns=['Followers']).reset_index()
        
        output_df['datetime_fetched'] = str(pd.to_datetime(datetime.today()))
        return output_df
            
                        


        
# %%
