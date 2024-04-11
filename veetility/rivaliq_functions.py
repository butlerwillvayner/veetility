import requests
import json
import pandas as pd
import os
from datetime import datetime
from veetility import snowflake as sf
import time
import io

def connect_to_snowflake(connection_parameters: dict):
    """
    Connect to Snowflake
    :param connection_parameters: Dictionary of connection parameters
    :return: Snowflake session
    """
    snowflake_session = sf.Snowflake(connection_params_dict=connection_parameters)
    return snowflake_session

def send_to_snowflake(connection_parameters: dict, 
                      df: pd.DataFrame, 
                      channel: str):
    """
    Send dataframe to Snowflake
    :param connection_parameters: Dictionary of connection parameters
    :param df: Pandas DataFrame
    :param channel: Twitter, Instagram, Facebook, Youtube, TikTok
    """
    table_names = { 'facebook' : "STG_RIVALIQ_FACEBOOK",
                    'twitter' : "STG_RIVALIQ_TWITTER",
                    'instagram' : "STG_RIVALIQ_INSTAGRAM",
                    'youtube' : "STG_RIVALIQ_YOUTUBE",
                    'tiktok' : "STG_RIVALIQ_TIKTOK",
                    'all' : "STG_RIVALIQ_ALL_SOCIAL_POSTS"
    }             
    snowflake_session = connect_to_snowflake(connection_parameters)
    # Convert dataframe values to str to avoid errors when sending to Snowflake
    df = df.astype(str)
    snowflake_session.write_df_to_snowflake(df=df, 
                                            database="VM_CORE_DATA_STAGING",
                                            schema="VM_RIVALIQ_STAGING", 
                                            table_name=table_names[channel],
                                            auto_create_table=True)
    

def print_pretty_json(json_obj):
    """
    Print JSON in a more readable format
    :param json_obj: JSON object
    """
    print(json.dumps(json_obj, indent=4, sort_keys=True))

def socialPosts_json_to_df(json_str):
    """
    Convert JSON string to Pandas DataFrame
    :param json_str: JSON string
    :return: Pandas DataFrame
    """
    data = json.loads(json_str)
    df = pd.json_normalize(data, 'socialPosts')
    return df

def get_socialPosts(landscapeId: str, 
                    apiKey: str,
                    companyId: str,
                    mainPeriodStart='2023-01-01', 
                    mainPeriodEnd=datetime.today().strftime('%Y-%m-%d'),
                    limit=500, 
                    channel='all', 
                    format='json', 
                    print_df=False, 
                    verbose=False, 
                    save_csv=False):
    """
    Returns the top 500 posts for for all companies within the landscape within a given time period
    Note: Rival IQ may add or reorder columns in the CSV outputs; callers should not depend on column order but rather on column name.
    :param landscapeId: Rival IQ landscape ID
    :param apiKey: Rival IQ API key
    :param companyId: Rival IQ company ID (if empty, returns all companies available to the landscape)
    :param mainPeriodStart: Start date of the main period
    :param mainPeriodEnd: End date of the main period
    :param limit: Number of posts to return (max 500)
    :param channel: Social media channel (all, facebook, twitter, instagram, tiktok, youtube)
    :param format: Format of the response (json or csv)
    :param print_df: Print the DataFrame
    :param verbose: Print the JSON response
    :param save_csv: Save the DataFrame as a CSV file
    """
    url = f"https://api.rivaliq.com/v3/landscapes/{landscapeId}/socialPosts"
    body = {
        'apiKey' : apiKey,
        'companyId' : companyId, # if empty, returns all companies available to the landscape
        'mainPeriodStart' : mainPeriodStart,
        'mainPeriodEnd' : mainPeriodEnd,
        'limit' : limit,
        'channel' : channel,
        'format' : format
    }
    response = requests.get(url=url, params=body)
    if response.status_code == 200:
        data = response.json()
        if verbose:
            print_pretty_json(data)
        df = socialPosts_json_to_df(response.text)
        if print_df:
            print(df)
            
        if save_csv:
            if companyId == '':
                companyId = 'all'
            directory = f"socialPosts/landscape_{landscapeId}"
            if not os.path.exists(directory):
                os.makedirs(directory)
            filename = f"socialPosts/landscape_{landscapeId}/socialPosts_data_landId_{landscapeId}_compIds_{companyId}_start_{mainPeriodStart}_end_{mainPeriodEnd}_channel_{channel}.csv"
            df.to_csv(filename, index=False)

    else:
        print(f"Error: {response.status_code}")
        print(response.text) 
        raise Exception(f"Error: {response.status_code}")


def get_bulkSocialPosts(landscapeId: str, 
                        apiKey: str, 
                        companyId: str = '', 
                        mainPeriodStart='2023-01-01', 
                        mainPeriodEnd=datetime.today().strftime('%Y-%m-%d'),
                        channel='all', 
                        format='json', 
                        save_csv=False):
    """
    Initiates retrieval of the social posts for all companies within the landscape within a given time period
    Note: Rival IQ may add or reorder columns in the CSV outputs; callers should not depend on column order but rather on column name.
    :param landscapeId: Rival IQ landscape ID
    :param apiKey: Rival IQ API key
    :param companyId: Rival IQ company ID (if empty, returns all companies available to the landscape)
    :param mainPeriodStart: Start date of the main period
    :param mainPeriodEnd: End date of the main period
    :param channel: Social media channel (all, facebook, twitter, instagram, tiktok, youtube)
    :param format: Format of the response (json or csv)
    :param save_csv: Save the DataFrame as a CSV file
    :param send_to_snowflake: Send the DataFrame to Snowflake
    :return: Pandas DataFrame if save_csv is False
    """
    url = f"https://api.rivaliq.com/v3/landscapes/{landscapeId}/bulkSocialPosts"
    body = {
        'apiKey' : apiKey,
        'mainPeriodStart' : mainPeriodStart,
        'mainPeriodEnd' : mainPeriodEnd,
        'channel' : channel,
        'format' : format
    }
    
    if companyId != '':
        body['companyId'] = companyId
    else:
        body['companyId'] = ''
        
    response = requests.get(url=url, params=body)
    if response.status_code == 202:
        token = response.json()['token']
        print(f'Bulk download token: {token}')
        start_time = time.time()
        link = check_bulkDownload_status(downloadToken=token, apiKey=apiKey, start_time=start_time)
        df = download_bulkSocialPosts_csv(link, landscapeId, companyId, mainPeriodStart, mainPeriodEnd, channel, save_csv)
        return df
    else:
        print(f"Error: {response.status_code}")
        print(response.text) 
        raise Exception(f"Error: {response.status_code}")
    
def get_bulkDownload_status(downloadToken: str, 
                            apiKey: str):
    """
    Checks bulk download status
    :param downloadToken: Bulk download token
    :param apiKey: Rival IQ API key
    :return: JSON response
    """
    url = f"https://api.rivaliq.com/v3/bulkDownload/{downloadToken}/status"
    body = {
        'apiKey' : apiKey
    }
    response = requests.get(url=url, params=body)
    if response.status_code == 200:
        return response
    else:
        print(f"Error: {response.status_code}")
        print(response.text) 
        raise Exception(f"Error: {response.status_code}")
    
  
def check_bulkDownload_status(downloadToken: str, 
                              apiKey: str, 
                              start_time: str):
    """
    Recursively checks bulk download status and returns the download link as soon as the status is ready (status == 2)
    :param downloadToken: Bulk download token
    :param apiKey: Rival IQ API key
    :start_time: Start time of the bulk download
    :return: Download link
    """
    response = get_bulkDownload_status(downloadToken, apiKey)
    if response.status_code == 200:
        data = response.json()
        status = data['status']
        if status == 2:
            print(f"Download link is ready! Elapsed time: {round(time.time() - start_time, 2)} seconds")
            return data['href']
        elif status == 3:
            print(f"Error: {response['status_code']}")
            print(response['text'])
            raise Exception(f"Download Failed! Elapsed time: {round(time.time() - start_time, 2)} seconds")
        else:
            print(f"Download is still in progress. Checking again in 60 seconds... Elapsed time: {round(time.time() - start_time, 2)} seconds")
            time.sleep(60) # wait for 20 seconds before checking again
            return check_bulkDownload_status(downloadToken, apiKey, start_time)
    else:
        print(f"Error: {response['status_code']}")
        print(response['text']) 
        raise Exception(f"Error: {response['status_code']}")        

def download_bulkSocialPosts_csv(link: str, 
                                 landscapeId: str, 
                                 companyId: str, 
                                 mainPeriodStart: str, 
                                 mainPeriodEnd: str, 
                                 channel: str, 
                                 save_csv = False):
    """
    Downloads a CSV file from a URL or returns a pandas dataframe
    :param link: URL of the CSV file
    :param landscapeId: Rival IQ landscape ID
    :param companyId: Rival IQ company ID (if empty, returns all companies available to the landscape)
    :param mainPeriodStart: Start date of the main period
    :param mainPeriodEnd: End date of the main period
    :param channel: Social media channel (all, facebook, twitter, instagram, tiktok, youtube)
    :param save_csv: If True, saves the file in the local directory. If False, returns a pandas dataframe.
    :param send_to_snowflake: If True, sends the file to Snowflake.
    :return: Returns a pandas dataframe.
    """
    # Sending a request to the URL
    r = requests.get(link)
    # Check if the request was successful
    if r.status_code == 200:
        # Open the file in write mode
        if companyId == '':
            companyId = 'all'
        if save_csv:
            directory = f"bulkSocialPosts/landscape_{landscapeId}"
            if not os.path.exists(directory):
                os.makedirs(directory)
            filename = f"bulkSocialPosts/landscape_{landscapeId}/bulkSocialPosts_data_landId_{landscapeId}_compIds_{companyId}_start_{mainPeriodStart}_end_{mainPeriodEnd}_channel_{channel}.csv"
            with open(filename, 'w') as file:
                # Writing the contents of the response to the file
                file.write(r.text)
            print("File downloaded successfully!")
        # Return pandas dataframe
        try:
            df = pd.read_csv(io.StringIO(r.text))
        except pd.errors.EmptyDataError:
            # empty dataframe
            print("Empty dataframe")
            df = None
        except pd.errors.ParserError:
            # CSV parsing error
            print("CSV parsing error")
            df = None
        except Exception:
            # catch all other exceptions
            df = None
        return df
    else:
        print("Unable to download the file. HTTP status code: ", r.status_code)
        
def get_available_landscapes(apiKey: str, 
                             verbose = False):
    """
    Get a list of available landscapes
    :param apiKey: Rival IQ API key
    :return: list of landscape IDsOh 
    """
    url = "https://api.rivaliq.com/v3/landscapes"
    
    body = {
        'apiKey' : apiKey,
    }
    response = requests.get(url=url, params=body)
    if response.status_code == 200:
        if verbose:
            print_pretty_json(response.json())
        return find_landscape_ids(response.text)
    else:
        print(f"Error: {response.status_code}")
        print(response.text) 
        raise Exception(f"Error: {response.status_code}")
    

def find_landscape_ids(json_string):
    """
    Finds and returns a list of landscape IDs from a JSON string
    :param json_string: JSON string
    :return: List of landscape IDs
    """
    data = json.loads(json_string)
    landscape_ids = [landscape['id'] for landscape in data['landscapes']]
    return landscape_ids

def get_landscapeCompanies(landscapeId: str, 
                           apiKey: str, 
                           verbose=False):
    """
    Get landscape companies 
    :param landscapeId: Rival IQ landscape ID
    :param apiKey: Rival IQ API key
    :return: List of company IDs in the given landscape
    """
    url = f"https://api.rivaliq.com/v3/landscapes/{landscapeId}/companies"
    
    body = {
        'apiKey' : apiKey,
    }
    
    response = requests.get(url=url, params=body)
    if response.status_code == 200:
        if verbose:
            print_pretty_json(response.json()) 
        return find_company_ids(response.text)
    else:
        print(f"Error: {response.status_code}")
        print(response.text) 
        raise Exception(f"Error: {response.status_code}")
    
def find_company_ids(json_string):
    """
    Finds and returns a dictionary of company names and ids from a JSON string
    :param json_string: JSON string
    :return: Dictionary of company names and ids in format {company_name: company_id}
    """
    data = json.loads(json_string)
    company_dict = {}
    for company in data['companies']:
        company_dict[company['name']] = company['id']
    return company_dict

def summaryMetrics_json_to_df(json_str):
    """
    Converts summary metrics JSON string to a dataframe
    :param json_str: JSON string
    :return: Dataframe
    """
    data = json.loads(json_str)
    df = pd.json_normalize(data, 'metrics')
    return df

def get_summaryMetrics(landscapeId: str, 
                       apiKey: str,
                       mainPeriodStart='2023-01-01', 
                       mainPeriodEnd=datetime.today().strftime('%Y-%m-%d'),
                       channel='all', 
                       format='json', 
                       print_df = True, 
                       verbose=False, 
                       save_csv=False): 
    """
    Returns summary values for all metrics
    :param landscapeId: Rival IQ landscape ID
    :param apiKey: Rival IQ API key
    :param mainPeriodStart: Start date of the main period
    :param mainPeriodEnd: End date of the main period
    :param channel: Social media channel (all, facebook, twitter, instagram, tiktok, youtube)
    :param format: Format of the output (json, csv)
    :param print_df: Print the dataframe
    :param verbose: Print the JSON response
    :param save_csv: Save the dataframe as a CSV file
    """
    url = f"https://api.rivaliq.com/v3/landscapes/{landscapeId}/metrics/summary"
    
    body = {
        'apiKey' : apiKey,
        'mainPeriodStart' : mainPeriodStart,
        'mainPeriodEnd' : mainPeriodEnd,
        'channel' : channel,
        'format' : format
    }
    response = requests.get(url=url, params=body)
    if response.status_code == 200:
        data = response.json()
        if verbose:
            print_pretty_json(data)
        df = summaryMetrics_json_to_df(response.text)
        
        if print_df:
            print(df)
            
        if save_csv:
            directory = f"summaryMetrics/landscape_{landscapeId}"
            if not os.path.exists(directory):
                os.makedirs(directory)
            filename = f"{directory}/summaryMetrics_data_landId_{landscapeId}_start_{mainPeriodStart}_end_{mainPeriodEnd}_channel_{channel}.csv"
            df.to_csv(filename, index=False)
            
    else:
        print(f"Error: {response.status_code}")
        print(response.text) 
        raise Exception(f"Error: {response.status_code}")
      

def post_followCompanies(apiKey: str, 
                         landscapeId: str, 
                         companyIds: str, 
                         verbose = False):
    """
    Follow companies in a given landscape. Uses their Rival IQ company IDs.
    At most 10 companies can be followed at a time.
    :param apiKey: Rival IQ API key
    :param landscapeId: Rival IQ landscape ID
    :param companyIds: List of company IDs
    :param verbose: Print the JSON response
    """
    # Check if the number of companies is less than 10
    if len(companyIds) > 10:
        raise Exception("At most 10 companies can be followed at a time.")
    
    # Verify that the company IDs are a list of integers
    if not all(isinstance(companyId, int) for companyId in companyIds):
        raise Exception("All company IDs must be integers.")
    
    # Append apiKey as a query parameter in the URL
    url = f"https://api.rivaliq.com/v3/landscapes/{landscapeId}/companies/byId?apiKey={apiKey}"
    
    body = {
        'companyIds' : companyIds,
    }
        
    response = requests.post(url=url, json=body)
    
    if response.status_code == 200:
        print("Companies followed successfully!")
        if verbose:
            print_pretty_json(response.json())
    else:
        print(f"Error: {response.status_code}")
        print(response.text) 
        raise Exception(f"Error: {response.status_code}")
    
def delete_unfollowCompany(apiKey: str,
                           landscapeId: str, 
                           companyId: str):
    """
    Unfollow company in the given landscape by its companyId
    :param apiKey: Rival IQ API key
    :param landscapeId: Rival IQ landscape ID
    :param companyId: company ID
    :param verbose: Print the JSON response
    """
    # Append apiKey as a query parameter in the URL
    url = f"https://api.rivaliq.com/v3/landscapes/{landscapeId}/companies/{companyId}?apiKey={apiKey}"
    
    response = requests.delete(url=url)
    
    if response.status_code == 204:
        print(f"Company with ID:{companyId} - unfollowed from landscapeId:{landscapeId}")
    else:
        print(f"Error: {response.status_code}")
        print(response.text) 
        raise Exception(f"Error: {response.status_code}")

def delete_unfollowAllCompanies(
    apiKey: str,
    landscapeId: str):
    """
    Unfollow ALL companies in the given landscape
    :param apiKey: Rival IQ API key
    :param landscapeId: Rival IQ landscape ID
    :param verbose: Print the JSON response
    """
    # Append apiKey as a query parameter in the URL
    url = f"https://api.rivaliq.com/v3/landscapes/{landscapeId}/companies?apiKey={apiKey}"
    
    response = requests.delete(url=url)
    
    if response.status_code == 204:
        print(f"All companies in landscape {landscapeId} - unfollowed")
    else:
        print(f"Error: {response.status_code}")
        print(response.text) 
        raise Exception(f"Error: {response.status_code}")