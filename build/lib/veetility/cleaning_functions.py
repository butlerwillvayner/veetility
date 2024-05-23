#%%
import pandas as pd
import regex as re
import logging
import os
import sys
from unidecode import unidecode
from collections import Counter
from . import utility_functions
from typing import Optional, Dict, List
pickle_path = "Pickled Files/"
# cleaning_logger = utility_functions.Logger('Indeed','CleaningFunctions')
DEFAULT_COLS_NO_CHANGE = [
    'spend', 'date', 'currency', 'cohort', 'creative_name', 'group_id',
    'engagements', 'created', 'ad_id', 'plays', 'saved', 'post_hastags',
    'content_type', 'linked_content', 'post_id', 'video_duration',
    'average_time_watched', 'total_time_watched', 'adset_targeting',
    'completion_rate', 'targeting', 'cohort_new', 'video_completions',
    'post_hashtags'
]
# %% -----------------------------
# Emojis to clean out of copy messages
# -----------------------------
emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)

def clean_column_names(
        df: pd.DataFrame, 
        name_of_df: str,
        hardcode_col_dict: Optional[Dict[str, str]] = None,
        on_error: str ='ignore',
        cols_no_change: List[str] = DEFAULT_COLS_NO_CHANGE,
        print_matches: bool = False
) -> pd.DataFrame:

    """Cleans the column names of an advertisement performance (organic or paid) dataset, commonly from
    Tracer but could also be from Sprout social. The column names will be standardized so
    then other functions in other libraries can work with the dataset.

    Args:
        df (DataFrame): Input dataframe containing a row item for each piece of creative or a day of advertising.
        name_of_df (str): The name of the dataframe, used in logging messages
        hardcode_col_dict (Dict[str, str], optional): A dictionary specifying exact transformations of column names
                                                        from the key to the value.
        errors (str, optional): How to handle errors during the conversion.
                                Can be 'raise', 'ignore' or 'warn'
        cols_no_change (List[str], optional): A list of column names to be left unchanged.

    Returns:
        DataFrame: Output dataframe with the column names standardized."""
    
    if hardcode_col_dict == None:
        hardcode_col_dict = {}
    
    new_columns = {}
    for column in df.columns:
        column = column.lower()
        hardcode_col_dict = {k.lower():v for k, v in hardcode_col_dict.items()} #make sure the keys are lowercase
        #hardcoded columns, for those unusual columns
        if column in hardcode_col_dict.keys():
            new_columns[column] = hardcode_col_dict[column]
            continue
        if 'day' == column:
            new_columns[column] = 'date'
        #Columns not to be changed and that don't get called similar things
        elif column in cols_no_change:
            new_columns[column] = column
            continue
        elif ('created' in column) and (('date' in column) or ('time' in column) or ('video' in column)):
            new_columns[column] = 'date'  # for organic
        elif ('video_create_time' in column):
            new_columns[column] = 'post_timestamp'  # TikTok organic
        elif ('like' in column) or ('favorite' in column) or ('reaction' in column):
            new_columns[column] = 'likes'
        elif ('video' not in column) and ('impression' in column) and ('unique' not in column):
            new_columns[column] = 'impressions'
        elif (column == 'reach') or (('impressions' in column) and ('unique' in column)):
            new_columns[column] = 'reach'
        elif ('campaign' in column) and ('name' in column):
            new_columns[column] = 'campaign_name'
        elif (('set' in column) or ('group' in column)) and ('name' in column):
            new_columns[column] = 'group_name'
        elif ('ad' in column) and ('name' in column):
            new_columns[column] = 'ad_name' # for TikTok organic
        elif ('creative' in column) and ('name' in column):
            new_columns[column] = 'creative_name'
        elif ('video' in column) and ('impression' in column):                       
            new_columns[column] = 'video_impressions'
        elif ('shares' in column) or ('retweet' in column):
            new_columns[column] = 'shares'
        elif (('conversion' in column) or ('lifetime'in column)) and ('save' in column): #_1d_click_onsite_conversion_post_save in fb_ig_paid data
            new_columns[column] = 'saved'
        
        elif ('video' in column) and ('view' in column) and not any(x in column for x in ['0', '5','2','3','6']): #don't include column with 25,50,75% completion
            new_columns[column] = 'video_views'
        elif ('video' in column) and ('25' in column):
            new_columns[column] = 'ad_video_views_p_25'
        elif ('video' in column) and ('50' in column):
            new_columns[column] = 'ad_video_views_p_50'
        elif ('video' in column) and ('75' in column):
            new_columns[column] = 'ad_video_views_p_75'
        elif ('video' in column) and (('100' in column) or ('complet' in column) or('full' in column)):
            new_columns[column] = 'video_completions'
        elif ('video' in column) and ('2' in column):
            new_columns[column] = 'ad_video_watched_2_s'
        elif ('video' in column) and ('3' in column):
            new_columns[column] = 'ad_video_watched_3_s'
        elif ('video' in column) and ('6' in column):
            new_columns[column] = 'ad_video_watched_6_s'
        
        elif ('organic' in column) and ('boosted' in column) or ( 'workstream' in column):
            new_columns[column] = 'workstream'
        elif 'currency' in column:
            new_columns[column] = 'currency'
        elif 'country' in column:
            new_columns[column] = 'country'
        elif ('replies' in column) or ('comment' in column):
            new_columns[column] = 'comments'
        elif (('page' in column) and('id' not in column)) or (column == 'profile') or ('business' in column) \
            or (('account' in column) and ('name' in column)) or (column == 'post_username'):
            new_columns[column] = 'account_name'  # for twitter organic
        elif ('caption' in column) or ('text' in column) or ('copy' in column) or ('message' in column) or(column == 'post') or (column == 'video_name'):
            new_columns[column] = 'message'
        elif (column == 'video_id') or ('post_id' in column) or ('ad_id' in column):
            new_columns[column] = 'post_id'
        elif ('url' in column) or (('link' in column) and ('clicks' not in column)):
            new_columns[column] = 'url'
        elif ('clicks' in column) and ('link' in column): #this is also equivalent to a swipe in Snapchat
            new_columns[column] = 'link_clicks'  # for all_plats_paid
        elif ('clicks' in column) and ('link' not in column):
            new_columns[column] = 'clicks'  # for all_plats_paid
        elif ('network' in column) or ('platform' in column):
            new_columns[column] = 'platform'  # for li_tt_igStories_organic
        elif (('media' in column) and ('product' in column) and ('type' in column)) or ('placement' in column):
            new_columns[column] = 'placement'
        elif (('type' in column) & ('post' in column)) or (('media' in column) and ('type' in column)) \
            or (('content' in column) and ('category' in column)): #creative media type for fb_ig_paid
            new_columns[column] = 'media_type'
        elif('cohort' in column):
            new_columns[column] = 'cohort'
        else:
            message = f'Column "{column}" in {name_of_df} not cleaned'
            new_columns[column] = column
            if on_error == 'raise':
                raise Exception(message)
            print(message)
            # cleaning_logger.logger.info(message)

    duplicated_cols = [item for item,count in Counter(list(new_columns.values())).items() if count > 1]
    # cleaning_logger.logger.info(f"{name_of_df} : {new_columns}")
    if len(duplicated_cols) > 0:
        error = f'Duplicate column names in {name_of_df} : {[f"{key} -> {value}" for key, value in new_columns.items() if value in duplicated_cols]}'
        # error = f'Duplicate column names in {name_of_df} : {[item for item, count in Counter(new_columns).items() if count > 1]}'
        # cleaning_logger.logger.exception(error)
        raise ValueError(error)
    df.columns = list(new_columns.values())

    if print_matches:
        print(f"{name_of_df} column matches = {new_columns}")

    return df

def extract_country_from_string(string: str,
                                client_name: str,
                                hardcode_dict: Optional[dict] = None) -> str:
    """Converts a string containing info identifying a certain 
    Args:
        string : str 
            string to pass through perhaps in a lambda function, the country is detected from this
        client_name : str 
            name of the client that is removed to make the detection of country easier, 
            for example so you are not searching for 'de' to find Germany with 'indeed' in the string
    Returns:
        country_code : str 
            country Tag abbreviation mostly following the ISO 3166-1 alpha-2 format"""
    string = str(string).lower().strip()
    hardcode_dict = {k.lower():v for k, v in hardcode_dict.items()}
    client_name = client_name.lower()
    # remove all non-alphanumeric characters
    if string in hardcode_dict.keys():
        country_code =  hardcode_dict[string]
    string = re.sub(r'[^\w\s]', '', string)
    string = string.replace(client_name, '')
    if ('uk' in string):
        country_code =  'UK/IE'
    elif ('ie' in string) or ('ireland' in string):
        country_code = "UK/IE"
    elif ('au' in string) or ('australia' in string):
        country_code = "AU"
    elif 'fr' in string:
        country_code = "FR"
    elif 'it' in string:
        country_code = "IT"
    elif 'nl' in string:
        country_code = "NL"
    elif 'de' in string:
        country_code = "DE"
    elif 'ca' in string:
        country_code = "CA"
    elif 'us' in string:
        country_code = "US"
    else:
        country_code = "N/A"
    return country_code

def strip_object_columns(df: pd.DataFrame):
    """Strips leading and trailing whitespaces in columns containing strings. 
        This stops effective duplications when two categories in a column 
        are essentially the same but one just has a whitespace"""
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    return df

def extract_region_from_country(country: str):
    """Extracts the region from the given country.

    Args:
        country (str): A string representing the country name.

    Returns:
        str: The region of the given country, if it is in 'UK/IE', 'NL', 'DE', 'FR', 'IT', 'BE',
            it returns "EMEA". If the country is 'CA' or 'US', it returns "N. America". If the
            country is not found, it returns None.
    Examples:
        >>> extract_region_from_country("FR")
        'EMEA'
        >>> extract_region_from_country("US")
        'N. America'
        >>> extract_region_from_country("XX")
        None
    """
    if country == 'UK/IE' or country == 'NL' or country == 'DE' \
            or country == 'FR' or country == 'IT' or country == 'BE':
        return "EMEA"
    elif country == 'CA' or country == 'US':
        return "N. America"
    else:
        return None


def clean_platform_name(platform: str) -> str:
    """Cleans the platform name into a the standard used company wide.

    Most platforms are in the 'title' case, i.e. First letter of each word is capitalised
    However there are some platforms that have are one word but are usually 
    presented as having a capital letter midway through the word. E.g. TikTok and LinkedIn"""
    
    platform = str(platform).lower()
    if 'tiktok' in platform:
        return 'TikTok'
    if 'linkedin' in platform:
        return 'LinkedIn'
    else:
        return platform.title()

def clean_url(url: str) -> str:
    """Clean the url of the post to produce a string with just the important information for matching.

    In the case of Tiktok remove everything after and including the ?, This removes the utm parameters"""
    url = str(url).lower().strip()
    url = url.replace('https://', '')
    url = url.replace('http://', '')
    url = url.replace('www.', '')
    url = url.replace('/', '')
    if 'tiktok' in url:
        return url.split('?')[0]
    return url

def updated_value_extract(url: str) -> str:
    """Extract the unique identifier for a post from the url
    This format of this code depends on the platform, sometimes
    it is a numerical code, sometimes it is alphanumeric"""

    try:
        if 'facebook' in url:
            u_url = re.search(r"/(\d{16})/|\w{0,4}/*(\d{11,16})", url).group(0)
            try:
                str(u_url)
            except:
                u_url = re.search(r".*/(\d[9])/$").group(0)
            return u_url
        elif 'instagram' in url:
            u_url = re.search(r"/(.{11})/", url).group(0)
            return u_url
        else:
            u_url = re.search(r"(\d{19})", url).group(0)
            return u_url
    except:
        return 'N/A'

def clean_media_type(media_type: str) -> str:
    """Clean the media type into a standardised version of media type"""
    media_type = str(media_type)
    # replace multiple underscores with a single space
    media_type = re.sub(r'_+', ' ', media_type)
    media_type = media_type.title().strip()

    if (media_type == 'Photo') or (media_type == 'Image') or (media_type == 'Static'):
        media_type_standardised = 'Image'
    elif ('Gif' in media_type) or ('Animated' in media_type):
        media_type_standardised = 'Gif'

    elif ('Native Templates' in media_type):
        media_type_standardised = 'Event'

    elif ('Video' in media_type):
        media_type_standardised = 'Video'

    elif ('Carousel' in media_type) or ('Album' in media_type):
        media_type_standardised = 'Carousel'

    elif media_type == 'Nan' or media_type == 'None' or media_type == 'Null' or media_type == '':
        media_type_standardised = 'N/A'
    else: #else leave it as it was
        media_type_standardised = media_type
    return media_type_standardised


def clean_placement(placement: str) -> str:
    """Clean a placement string into a standardised placement
    Args:
        placement (str): A string representing the placement of the post 
    Returns:
        placement (str): The standardised placement of the post, if it is in 'Reel', 'Story' or 'Feed'"""
        
    placement = str(placement).title().strip()
    if placement == 'Nan' or placement == 'None' or placement == 'Null' or placement == '':
        placement = 'Feed'
    elif ('Reel' in placement):
        placement = 'Reel'
    elif 'Story' in placement:
        placement = 'Story'
    else:
        placement = 'Feed'
    return placement

def extract_quarter_from_date(date: str) -> str:
    """Extracts the quarter from the date"""
    date = pd.to_datetime(date)
    return date.quarter

def extract_after_nth_occurrence(string: str, 
                                 char: str, 
                                 n: int
) -> str:
    """Extracts the string between the nth and n+1th occurrence of the character"""
    extract = string.split(char)[n]
    extract = extract.strip()
    extract = extract.title()
    return extract

def extract_creative_name(name, group_name):
    "Extract the creative name from either the Ad name or the group name depending on which is available"
    asset_name_from_name = extract_value(name, 'a')
    asset_name_from_group = extract_value(group_name, 'ad')

    if asset_name_from_name == None:
        return asset_name_from_group
    else:
        return asset_name_from_name

def extract_value(string, identifier):
    """Extract value based on naming convention identifier"""
    try:
        string = string+'_'
        search_string = f'_{identifier}:(.*?)_'
        value = re.search(search_string, string).group(1)
        value = value.upper()  # all keys in naming convention are upper case
        if value == '':
            value = None
    except:
        value = None
    return value


def two_urls_per_post_to_1(x, target_cols=None):
    """This function deals with posts that have two urls even though they are effectively the same post.

    Sometimes social media posts are posted but then deleted, only to be reposted later. 

    This function returns just the url of the post with the highest amount of 'impressions' or 'video views'

    You can take a dataframe of social media posts, groupby the unique identifier and this will return only 
    the row item with the highest number of impressions or video_views. 
    
    """
    if target_cols is None:
        target_cols =   ['url', 'influencer?']

    if x['platform'].iloc[0] != 'TikTok':
        target_col = 'impressions'
        max_score = x['impressions'].max()
    else:
        target_col = 'video_views'
        max_score = x['video_views'].max()
        
    # Check if max_score is zero
    if max_score == 0:
        return pd.DataFrame(columns=target_cols)  # Return empty DataFrame


    return x[x[target_col] == max_score][target_cols]


# for ID_Organic__CA_2022_Q2_USD_ENG_TW
def extract_columns_twitter_2(df):
    """Special case of extracting columns from unique Twitter campaign naming convention"""
    df['Group Name'] = df['Group Name'].str.lower()
    df['workstream'] = df['Group Name'].apply(
        lambda x: extract_after_nth_occurrence(x, '_', 1))
    df['country'] = df['Group Name'].apply(
        lambda x: extract_after_nth_occurrence(x, '_', 3))  # double underscore
    df['cohort'] = df['Group Name'].apply(
        lambda x: extract_after_nth_occurrence(x, '_', 5))
    df['Creative Name'] = df['Group Name'].apply(
        lambda x: extract_after_nth_occurrence(x, 'TW'))
    return df


def video_len_toseconds(video_length) -> int:
    """Convert video length in format from Tracer usually "1:20" to seconds (80 seconds in this case)

    Args:
        video_length (str or int): A string representing video length, 
            which can be in the format of 'minutes:seconds' or 'minutes.seconds', 
            or an integer representing the video length in seconds. 

    Returns:
        int: The video length in seconds.
            Returns None if the input does not make sense 
            (for example, negative times, or strings that don't match the expected formats).
    
    Examples:
        >>> video_len_toseconds('1:20')
        80
        >>> video_len_toseconds('0.20')
        20
        >>> video_len_toseconds('20')
        20
        >>> video_len_toseconds(30)
        30
    """

    if isinstance(video_length, int):
        if video_length < 0:
            return None
        return video_length

    video_length = video_length.strip()

    try:
        # Check if the string contains a ":"
        if ':' in video_length:
            minutes, seconds = map(int, video_length.split(':'))
        # Check if the string contains a "."
        elif '.' in video_length:
            minutes, seconds = map(int, video_length.split('.'))
        # If no ":" or "." are present, then it's just seconds
        else:
            seconds = int(video_length)
            minutes = 0
    except ValueError:
        return None

    if minutes < 0 or seconds < 0:
        return None

    return minutes * 60 + seconds