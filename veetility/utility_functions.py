#%%
import requests
import json
import pandas as pd
import regex as re
import numpy as np
import time
from fuzzywuzzy import fuzz
from tqdm.auto import tqdm
import psycopg2 as pg
import sys
import logging
import gspread
import pickle
import gspread_dataframe as gd
import os
import subprocess
import sqlalchemy as sa
from unidecode import unidecode
from datetime import datetime, timedelta

#%%

emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)


class UtilityFunctions():
    """A general purpose class for performing python pipeline functions such as reading/writing to google sheets, postgreSQL databases
    storing data as pickle or JSON files, with error handling and automated retries.
     
    Also includes more complicated functions such as for merging paid and organic social data using fuzzy matching and regex or 
    storing cumulative data we receieve as a daily incremental total"""

    def __init__(
            self, client_name, 
            gspread_auth_dict=None, db_user=None, 
            db_password=None, db_host=None,
            db_port=None, db_name=None, 
            log_name='utility_functions'):
        """Initialise a google sheets connector and postgreSQL connector for the utility instance 

        This means you can only connect to one google account and one database per instance 
        of the UtilityFunctions class.

        The email address of the google account must be added to the google sheet as a collaborator
        
        Args:
            client_name (str): Used to specify the folder, 
            gspread_auth_dict (dict): A dictionary containing google authorisation data
            db_user (str): The postgreSQL database username
            db_password (str): The postgreSQL database password
            db_host (str): The postgreSQL database host url
            db_port (str): The postgreSQL database port number as a string, usually 5432
            db_name (str): The postgreSQL database name
        Returns:
            None"""
        if gspread_auth_dict != None:
            self.sa = gspread.service_account_from_dict(gspread_auth_dict)         
        if db_user != None:
            self.db_user, self.db_password, self.db_host, self.db_port, self.db_name = \
            db_user, db_password, db_host, db_port, db_name
            postgres_str = f'postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}'
            self.postgresql_engine = sa.create_engine(postgres_str)
        self.client_name = client_name
        # Initialise a logger for the utility functions
        self.logger = Logger(client_name, log_name)
    
    def prepare_string_matching(self, 
                                string, 
                                is_url=False, 
                                readable_form=False, 
                                ascii_characters='remove',
                                remove_newlines=True):
        """Removing unnecessary detail, whitespaces and converting to lower case.
        
        Prepare strings for matching say in a merge function by removing unnecessary 
            detail, whitespaces and converting to lower case.

            Remove URLs and emojis as sometimes they cannot come through properly in Tracer data
            Replace non-ASCII characters with their closest ASCII equivalents

            Parameters
            -----------------
            string : str 
                The string to be cleaned
            is_url : bool 
                If True then remove URLs and characters after the '?' which are utm parameters
                These can be present in some URLs we receive and not others
            Returns 
            ----------------
            string : str
                A cleaned string stripped of whitespace, punctuation, emojis, non-ASCII characters, and URLs."""
        
        if pd.isna(string):
            return string
        
        if readable_form == False: 
            string = str(string).lower() # Convert the input to a string and make it lower case
            string = string.replace(' ', '')
            string = re.sub(r'[^\w\s]', '', string) # remove punctuation
            string = string.replace(' ', '').replace('\n', '') # remove spaces and newline characters
        
        if is_url:
            # Remove URLs and characters after the '?'
            string = string.split('?')[0] # Get rid of everything after they start to be utm parameters

        else: #this is commonly for a post message
            string = string + ' ' # add a space to the end of the string so that the regex below works            
            string = re.sub(r'(https?://\S+)\s', '', string) # Remove URLs up to the first whitespace

        string = emoji_pattern.sub(r'', string) # remove emojis
    
        if ascii_characters == 'replace':
            string = unidecode(string)  # replace non-ASCII characters with their closest ASCII equivalents
        if ascii_characters == 'remove':
            string = re.sub(r'[^\x00-\x7F]+', '', string)
        
        if remove_newlines == True:
            string = string.replace('\n', '') # remove spaces and newline characters
        
        return string

    def match_ads(
            self, df_1, 
            df_2, df_1_exact_col, 
            df_2_exact_col, extract_shortcode=False,
            df_1_fuzzy_col=None, df_2_fuzzy_col=None, 
            is_exact_col_link=True, matched_col_name='boosted', 
            merge=False, cols_to_merge=None, 
            pickle_name='NoStore', fuzz_thresh=80):
        """Match row items in df_2 onto row items in df_1 based on two sets of columns,using exact and fuzzy matching.

        First try to match the row items in df_1 using the first set of columns, if there is no match then try to match
        the row items in df_2 using the second set of columns and fuzzy matching.
        For example the first set of columns might be URLs, which tend to be exact matches, and the second set of columns
        might be post copy, which can have slight variations, for example the post copy from a tracker sheet might be
        slightly incorrect due to manual entry, therefore fuzzy matching with a threshold of how similar the strings
        need to be is used.
        
        Args:
            df_1 (DataFrame): The Dataframe that will be searched to see if any corresponding values in df_2, if merge=True df_2 will be left joined onto df_1
            df_2 (DataFrame): The Dataframe that if merge = True will be left joined onto df_1
            df_1_exact_col (str): The Column name from df_1 that will be first attempted to find exact matches
            df_2_exact_col (str): The Column name from df_2 that will be first attempted to find exact matches
            extract_shortcode (bool): Boolean Flag, if True then the exact match will be attempted on the shortcodes of the df_2_exact_col which should be a url
            df_1_fuzzy_col (str): The Column name from df_1 that will be attempted to fuzzy match if there was no exact match before.
            df_2_fuzzy_col (str): The Column name from df_2 that will be attempted to fuzzy match if there was no exact match before.
            is_exact_col_link (bool): Boolean Flag, is the set of columns to be exact matched hyperlinks? If so they will be cleaned to remove utm parameters.
            matched_col_name (str): String to name the column which will contain boolean values to indicate whether row items in df_1 found a match in df_2.
            merge (bool): Boolean Flag, if true then df_2 will be left joined onto df_1. Else df_1 will be left unchanged apart from column indicating whether there is a match.
            cols_to_merge (list, str): List of strings to merge on if 'merge' = True.
            pickle_name (str): Name of the dictionary of best matches found by fuzzy matching to be stored as a pickle file. The next time the function is run with the same pickle_name, the pickle file is used to find matches without having to do slow fuzzy matching from scratch.
            fuzz_thresh (int): Fuzzy matching threshold value between 0 and 100
            
        Returns:
            df_1 (DataFrame): The original df_1 with just a column to indicate whether a match has occured if merge = False else df_1 will have df_2 left joined on.
            df_2 (DataFrame): The original df_2 with cleaned columns and 'Match String' column to help quality check why some rows have or haven't matched.
            df_2_no_match (DataFrame): A dataframe of df_2 row items that haven't found a match in df_1."""
        
        df_1 = df_1.drop(columns=[col for col in df_1.columns if ('matched' in col) and ('df' in col)])
        df_2 = df_2.drop(columns=[col for col in df_2.columns if ('matched' in col) and ('df' in col)])

        if cols_to_merge == None: cols_to_merge = ['platform']

        if df_1_fuzzy_col == None: # If only oen set of columns then first do an exact match then a fuzzy match on just that set of columns
            df_1_fuzzy_col = df_1_exact_col
            df_2_fuzzy_col = df_2_exact_col

        # Prepare strings in a raw form with no spaces or punctuation in order to increase chance of matching
        df_1['match_string'] = df_1[df_1_exact_col].apply(lambda x: self.prepare_string_matching(x, is_url=is_exact_col_link))
        df_2['match_string'] = df_2[df_2_exact_col].apply(lambda x: self.prepare_string_matching(x, is_url=is_exact_col_link))

        # Find out the number of unique values of the first cleaned column to match by
        df_1_unique_exact = df_1['match_string'].unique().tolist()
        df_2_unique_exact = df_2['match_string'].unique().tolist()

        if extract_shortcode:
            # Create a dictionary of mappings between urls in df_2 and shortcodes in df_1
            url_shortcode_dict = self.match_shortcode_to_url(df_1_unique_exact, df_2_unique_exact)
            df_2['match_string'] = df_2['match_string'].map(url_shortcode_dict)

        cols_to_merge.append('match_string')

        # Identify matches between the two dataframes on cols_to_merge,e.g. is there a row with 'platform' and 'message' in df_1 that matches a row in df_2
        df_1, df_2 = self.identify_match_multi_cols(df_1, df_2, cols_to_merge, cols_to_merge, 'matched_exact')

        df_1[matched_col_name] = False
        df_1['matched_fuzzy_df1?'] = False

        # Split Dataframes up into rows that had a URL match and one where the rows didn't match
        # Then the ones that didn't match will try and be matched with the cleaned caption
        df_1_match = df_1[df_1['matched_exact_df1?'] == True]
                
        # Exact column merge match
        # We don't match on match_id because we don't want the cols_to_merge to be duplicated with _x and _y
        if merge:
            df_1_match = self.merge_match_perc(df_1_match, df_2, on=cols_to_merge, 
                                                    how='left', tag="First set of columns exact match")
        
        # Now the match string will be based off the column to be fuzzy matched    
        df_1['match_string'] = df_1[df_1_fuzzy_col].apply(lambda x: self.prepare_string_matching(x))
        df_2['match_string'] = df_2[df_2_fuzzy_col].apply(lambda x: self.prepare_string_matching(x))
        
        # now create a dataframe of the rows that didn't have an exact match and try and fuzzy match them
        df_1_no_match = df_1[df_1['matched_exact_df1?'] == False]
        
        # Find the unique instances of cleaned captions to be passed to the fuzzy matching function
        df_1_no_match_unique = df_1_no_match['match_string'].unique().tolist()
        df_2_fuzzy_unique = df_2['match_string'].unique().tolist()

        # the fuzzy match function will return a dictionary of matches for each caption from df_1 with the value
        # being the fuzzy col of df_2 with the best match above a certain percentage threshold similarity
        best_match_dict = self.best_fuzzy_match(df_1_no_match_unique, df_2_fuzzy_unique, fuzz_thresh, pickle_name)

        # create a column that is the closest match in df_2 for every caption in df_1
        # This will be used to merge df_2 onto the remainder of none matching df_1
        df_1_no_match['match_string'] = df_1_no_match["match_string"].map(best_match_dict)

        # Identify matches between the two dataframes on cols_to_merge,e.g. is there a row with 'platform' and 'message' in df_1 that matches a row in df_2
        df_1_no_match, df_2 = self.identify_match_multi_cols(df_1_no_match, df_2, cols_to_merge, cols_to_merge, 'matched_fuzzy')
        
        # Fuzzy match merge the rows that didn't match on the exact column
        if merge:
            df_1_no_match = self.merge_match_perc(df_1_no_match, df_2.drop([df_2_fuzzy_col, 'matched_exact_df2?', 'matched_fuzzy_df2?'], axis=1),
                                        on=cols_to_merge, how='left', tag="Second set of columns fuzzy match")
        
        df_1 = pd.concat([df_1_match, df_1_no_match], ignore_index=True)
        
        # The matched_col_name indicates whether a match either exact or fuzzy was found
        df_1[matched_col_name] = df_1.apply(lambda x: True if ((x['matched_exact_df1?'] == True) or (x['matched_fuzzy_df1?'] == True)) else False, axis=1)
        df_2[matched_col_name] = df_2.apply(lambda x: True if ((x['matched_exact_df2?'] == True) or (x['matched_fuzzy_df2?'] == True)) else False, axis=1)

        self.logger.logger.info(f"Num unique exact col values in df_1 = {df_1[df_1_exact_col].nunique()}")
        self.logger.logger.info(f"Num unique fuzzy col values in df_1 = {df_1[df_1_fuzzy_col].nunique()}")
        self.logger.logger.info(f"Num unique exact col values in df_2 = {df_2[df_2_exact_col].nunique()}")
        self.logger.logger.info(f"Num unique fuzzy col values in df_2 = {df_2[df_2_fuzzy_col].nunique()}")
        self.logger.logger.info(f"Perc Matched df_1 exact col = {round(df_1['matched_exact_df1?'].sum()*100/df_1.shape[0], 2)}")
        self.logger.logger.info(f"Perc Matched df_1 fuzzy col = {round(df_1['matched_fuzzy_df1?'].sum()*100/df_1.shape[0], 2)}")
        self.logger.logger.info(f"Perc df_2 exact col could have matched = {round(df_2['matched_exact_df2?'].sum()*100/df_2.shape[0], 2)}")
        self.logger.logger.info(f"Perc df_2 fuzzy col could have matched = {round(df_2['matched_fuzzy_df2?'].sum()*100/df_2.shape[0], 2)}")
        self.logger.logger.info(f"Perc Matched df_1 matched = {round(df_1[matched_col_name].sum()*100/df_1.shape[0], 2)}")
        self.logger.logger.info(f"Perc Matched df_2 matched = {round(df_2[matched_col_name].sum()*100/df_2.shape[0], 2)}")

        return df_1, df_2
    
    def identify_match_multi_cols(
            self, df_1, 
            df_2, df_1_cols_to_match, 
            df_2_cols_to_match, match_col_name, 
            exclude_values=None):
        """This function will identify if a row in df_1 is in df_2 based on the columns specified in df_1_cols_to_match and df_2_cols_to_match
        
        Args:
            df_1 (pd.DataFrame): The first dataframe to identify matches in
            df_2 (pd.DataFrame): The second dataframe to identify matches in
            df_1_cols_to_match (list): The columns in df_1 to look for matches in df_2
            df_2_cols_to_match (list): The columns in df_2 to look for matches in df_1
            match_col_name (str): The name of the column to be created in df_1 to indicate if the row is in df_2
            exclude_values (list): The values to be excluded from the match. Default is ['None', 'none', 'nan', '']"""
        if exclude_values == None:
            exclude_values = ['None', 'none', 'nan', '']
        
        def is_row_in_dataframe(row, target_df, source_cols, target_cols):
            mask = np.full(len(target_df), True, dtype=bool)
            for i in range(len(source_cols)):
                current_condition = (
                    (target_df[target_cols[i]] == row[source_cols[i]]) &
                    (row[source_cols[i]] not in exclude_values)) | (
                    pd.isna(target_df[target_cols[i]]) & pd.isna(row[source_cols[i]]))
                mask &= current_condition
            matches = target_df[mask]
            return len(matches) > 0

        df_1[match_col_name + '_df1?'] = df_1.apply(lambda row: is_row_in_dataframe(row, df_2, df_1_cols_to_match, df_2_cols_to_match), axis=1)
        df_2[match_col_name + '_df2?'] = df_2.apply(lambda row: is_row_in_dataframe(row, df_1, df_2_cols_to_match, df_1_cols_to_match), axis=1)

        return df_1, df_2

    def best_fuzzy_match(
            self, list_1, 
            list_2, threshold, 
            json_name
    ):
        """Takes in two lists of strings and every string in list_1 is fuzzy matched onto every item in list_2
        The fuzzy match of a string in list_1 with a string in list_2 with the highest score will count as the 
        match as long as it is above the threshold. The match is then stored as a key value pair in a dictionary

        The dictionary of matches will be saved as a pickle file to be used next time the function is run to save
        having to do searches on a string if we've already found a match in the past

        Args:
            list_1 (list): First List of strings, every item will be searched for a fuzzy match in list_2
            list_2 (list): Second List of strings, every item in list_1 will be fuzzy matched with every item in list 2 and best fuzzy match score wins
            threshold (integer): value between 0 and 100 signifying percentage fuzzy match score at which a match is considered sufficiently close
            json_name (str): Name of json file to store dictionary of matches in
        
        Returns:
            best_match_dict (dict): Dictionary of matches (with highest fuzzy match score) between strings in list_1 and list_2 
                    key = string in list_1, value = string in list_2 """

        best_match_dict = {} 
        stored_best_dict = {}
        base_log_dir = os.environ.get('LOG_DIR')
        # Create a client specific log directory
        client_log_dir = os.path.join(base_log_dir, self.client_name)
        json_folder = os.path.join(client_log_dir, 'JSON Files')
        if not os.path.exists(json_folder):
            os.makedirs(json_folder)
        
        if json_name != 'NoStore':
            if os.path.exists(os.path.join(json_folder, f'best_match_dict_{json_name}.json')):
                stored_best_dict = self.read_json(f'best_match_dict_{json_name}', 'Dictionary', json_folder)
                self.logger.logger.info(f"loaded dict of len :{len(stored_best_dict)}")

        for string_1 in tqdm(list_1):
            
            if string_1 == '':
                best_match_dict[''] = 'None'
                continue
            # If there is an exact match then just put the match as itself and no need to go through list
            if string_1 in list_2:  
                best_match_dict[string_1] = string_1
                continue
            
            #If there is a match in the stored dictionary then use that
            if string_1 in list(stored_best_dict.keys()): 
                best_match_dict[string_1] = stored_best_dict[string_1]
                continue
            
            #Then go through every string in the second list and fuzzy match to produce a score
            #If the score is below the threshold then set the score to 0
            temp_match_dict = {}
            for string_2 in list_2:
                score = fuzz.ratio(string_1, string_2)
                if score < threshold:
                    score = 0
                temp_match_dict[string_2] = score
            
            #if there were no matches above the threshold then return a match for that 
            #string in list_1 equal to "none"
            if max(temp_match_dict.values()) == 0:
                best_match_dict[string_1] = 'None'
            else:
                # find the match with the highest matching score and save that to the
                # best match dictionary
                best_match = max(temp_match_dict.items(), key=lambda k: k[1])[0]
                best_match_dict[string_1] = best_match

        if json_name != 'NoStore':
            # Remove matches that didn't find anythign as that will let new values be discovered
            # if new data comes in
            self.write_json(best_match_dict, f'best_match_dict_{json_name}', 'Dictionary', json_folder)

        return best_match_dict

    def write_to_postgresql(
            self, df, 
            table_name, if_exists='replace'):
        """Writes a dataframe to a PostgreSQL database table using a SQLalchemy engine defined elsewhere.
        If writing fails it waits 10 seconds then trys again
            
        Args:
            df (DataFrame): The Dataframe to send to the PostGreSQL table
            table_name (str): The name of the table to write the dataframe to
            if_exists (str): Either 'replace' or 'append' which describes what to do if a table with that name already exists
                                    
        Returns:
            error_message (str): An error message saying that the connection has failed """
        # create a SQL alchemy engine to write the data to the database after cleaning
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        df['DateWrittenToDB'] = dt_string
        error_message = ''
        try:
            now = time.time()
            if 'index' in df.columns:
                df = df.drop('index', axis=1)
            df.to_sql(table_name, con=self.postgresql_engine, index=False, if_exists=if_exists)
            time_taken = round(time.time() - now, 2)
            self.logger.logger.info(f"Time Taken to write {table_name} = {time_taken}secs")
            self.logger.logger.info(f"Sent Data to {table_name}")
        except Exception as e:
            self.logger.logger.info(f"Connection error {e}") 
            time.sleep(10) # Wait for 10 seconds then try again
            try:
                now = time.time()
                df.to_sql(table_name, con=self.postgresql_engine, index=False, if_exists=if_exists)
                time_taken = round(time.time() - now,2)
                self.logger.logger.info(f"Time Taken to write {table_name} = {time_taken}secs")
            except Exception as error_message:
                self.logger.logger.error(f'Connection failed again {error_message}', exc_info=True)
                return f'{table_name} error: ' + str(error_message)
        return error_message

    def store_daily_organic_data(
            self, df, output_table_name, 
            num_days_to_store=30, date_col_name='date',
            dayfirst="EnterValue", yearfirst="EnterValue", 
            format=None, errors='raise',
            check_created_col=True, created_col='created', 
            refresh_lag=1, cumulative_metric_cols=None, 
            unique_id_cols=None,
            require_run_after_hour=False, run_after_hour=15):
        """Converts a post level organic dataframe to a daily level dataframe and stores it in a PostGreSQL table.
        
        Most organic data is stored at the post level and this function converts it to a daily level dataframe
        and stores it in a PostGreSQL table. It also converts the cumulative metrics to daily difference metrics.
        The date column is parsed through with the correct format, dayfirst and yearfirst values needing to be specified.
        If the table already exists then it checks the date_updated column to see if the data has already been
        updated today. If it has then it doesn't update the table. If it hasn't then it updates the table.
        If the table doesn't exist then it creates it.
        If the require_run_after_hour is set to True then it will only run if the current time is after the
        run_after_hour time which is in 24 hour format but only the hour is used.
        If check_created_col is set to True then it will only run if the created column is less than the
        refresh_lag days ago. This is to ensure that the data is up to date before it is stored. This "created" 
        columns appears in databases from tracer and tells us when Tracer last updated the row items. Tracer is 
        a day behind hence the refresh_lag of 1 day.
        The date_row_added column is added to the dataframe and is the date that the row was added to the
        data output_table.
        The date_first_tracked column is added to the dataframe and is the date that a unique post as defined
        by the unique_id_cols was first tracked
        The date_diff column is added to the dataframe and is the number of days difference between when the post
        was first tracked and when that particular row item was added to the data output_table.


        Args:
            df (DataFrame): The dataframe to be converted to a daily level dataframe and stored in a PostGreSQL table
            output_table_name (str): The name of the table to store the data in
            num_days_to_store (int, optional): The number of days worth of data per post to store in the table. Defaults to 30.
            date_col_name (str, optional): The name of the date column in the dataframe that will be formatted. Defaults to 'date'.
            dayfirst (str, optional): Whether the day is the first value in the date column. Defaults to "EnterValue".
            yearfirst (str, optional): Whether the year is the first value in the date column. Defaults to "EnterValue".
            format (str, optional): The format of the date column. Defaults to None.
            errors (str, optional): How to handle errors in the date column. Defaults to 'raise'.
            check_created_col (bool, optional): Whether to check the created column to ensure the data is up to date. Defaults to True.
            created_col (str, optional): The name of the created column. Defaults to 'created'.
            refresh_lag (int, optional): The number of days to check the created column is less than. Defaults to 1.
            cumulative_metric_cols (list, optional): The list of cumulative metrics to convert to daily difference metrics. Defaults to ['impressions','reach','video_views','reactions','comments','shares'].
            unique_id_cols (list, optional): The list of columns that uniquely identify a post. Defaults to None.
            require_run_after_hour (bool, optional): Whether to only run the function if the current time is after the run_after_hour time. Defaults to False.
            run_after_hour (int, optional): The hour of the day to run the function after in 24 hour format. Defaults to 15.
        
        Returns:
            None: the function writes the data to a postgresql table """
        if cumulative_metric_cols == None:
            cumulative_metric_cols = ['impressions', 'reach', 'video_views',
                                      'comments', 'shares'
                                      ]
        today_datetime = datetime.today()
        today_date = today_datetime.date()
        if require_run_after_hour and (today_datetime.hour < run_after_hour): 
            self.logger.logger.info("Require run after hour is set to True and it is before the run after hour time")
            return
        #Check Tracer data has actually updated
        if self.table_exists(output_table_name):
            old_df = self.read_from_postgresql(output_table_name, clean_date=True, date_col=date_col_name,
                                               dayfirst=dayfirst, yearfirst=yearfirst, format=format, errors=errors)
            if old_df['date_row_added'].max().date() == today_date:
                self.logger.logger.info(f"It looks data has already pushed to {output_table_name} today")
            else:
                if (df[created_col].max().date() < today_date - timedelta(days=refresh_lag)) and (check_created_col):
                    error_message = "It looks like the input df has not been updated yesterday. Therefore there is no fresh data to add on."
                    self.logger.logger.info(error_message)
                    raise Exception(error_message)
                cutoff_date = today_date - timedelta(days=num_days_to_store)
                
                df[date_col_name] = pd.to_datetime(df[date_col_name], dayfirst=dayfirst, yearfirst=yearfirst,
                                        format=format, errors=errors)
                df = df[df[date_col_name].dt.date >= (cutoff_date)] # Filter data only after the cutoff date
                df['date_row_added'] = today_datetime
                df['date_diff'] = (df['date_row_added'] - df[date_col_name]).dt.days
                df['date_first_tracked'] = df.groupby(unique_id_cols)['date_row_added'].transform('min')
                df = pd.concat([df, old_df])
                for metric in cumulative_metric_cols: 
                    df['cum_'+metric] = df[metric] # Set the cumulative metrics to the same value as the daily metrics
                
                self.write_to_postgresql(df, output_table_name, if_exists='replace')

                daily_df = self.convert_cumulative_to_daily(df, cumulative_metric_cols, unique_id_cols, 'date_row_added')
                
                self.write_to_postgresql(daily_df, output_table_name + '_daily_conv', if_exists='replace')

        else: #if the table doesn't exist create it with the whole dataset for the first time
            df[date_col_name] = pd.to_datetime(df[date_col_name], dayfirst=dayfirst, yearfirst=yearfirst,
                                        format=format, errors=errors)
            df['date_row_added'] = today_datetime
            df['date_first_tracked'] = today_datetime
            df['date_diff'] = (df['date_row_added'] - df[date_col_name]).dt.days
            
            self.write_to_postgresql(df, output_table_name, if_exists='replace')

            daily_df = self.convert_cumulative_to_daily(df, cumulative_metric_cols, unique_id_cols, 'date_row_added')
            
            self.write_to_postgresql(daily_df, output_table_name + '_daily_conv', if_exists='replace')


    def convert_cumulative_to_daily(
            self, df, 
            metric_list = None,
            unique_identifier_cols='url', 
            date_row_added_col='date_row_added'
    ):
        """Convert cumulative metrics to daily metrics for a given dataframe.

        Args:
            df (DataFrame): The dataframe to convert the cumulative metrics to daily metrics
            metric_list (list, optional): The list of metrics to convert. Defaults to ['impressions','comments','clicks',
                                    'link_clicks','likes','saved','shares','video_views'].
            unique_identifier_cols (list, optional): The list of columns that uniquely identify a post. Defaults to 'url'.
            date_row_added_col (str, optional): The name of the column that contains the date the row was added to the dataframe. Defaults to 'date_row_added'.
        Returns:
            df (DataFrame): The dataframe with the cumulative metrics converted to daily metrics"""
        
        if metric_list == None:
            metric_list = [
                'impressions', 'comments', 'clicks',
                'link_clicks', 'likes', 'saved', 
                'shares', 'video_views'
            ]
        #rename the metric list to create by appending 'cum_' to the start of each metric
        cum_metric_list = ['cum_' + metric for metric in metric_list]
        #Check if the cumulative metrics have been calculated before
        if any(['cum_' in x for x in df.columns]):
            conversion_run_before = True
            previous_cum_metrics = [x for x in df.columns if 'cum_' in x]
            if previous_cum_metrics != cum_metric_list:
                raise Exception("The cumulative metrics in the dataframe do not match the cumulative metrics in the metric list\
                                 input parameter")
        else:
            conversion_run_before = False

        if conversion_run_before == False:
            # If the cumulative metrics have not been calculated before then 
            # Duplicate the metrics from the dataframe as by default the metrics are cumulative
            for metric in metric_list:
                #set the cumulative metrics to the same value as the daily metrics
                df['cum_' + metric] = df[metric]
                cum_metric_list.append('cum_' + metric)
            return df
            
        if isinstance(unique_identifier_cols, list) == False: # If the entry is a string just convert it to a list
            unique_identifier_cols = [unique_identifier_cols]
        # Sort the dataframe by the unique identifier columns and the date the row was added
        df = df.sort_values(by=unique_identifier_cols+[date_row_added_col])
        # Calculate the daily metrics by subtracting the previous day's cumulative metric from the current day's cumulative metric
        metrics_updated = df.groupby(unique_identifier_cols)[cum_metric_list].transform(lambda x:x.sub(x.shift().fillna(0))).reset_index()
        df[metric_list] = metrics_updated
        df_metrics = df[metric_list]
        # Set negative values to zero, cumulative totals can decrease, potentially due to people accidently liking posts
        df_metrics[df_metrics < 0] = 0
        df[metric_list] = df_metrics
        return df
    
    def get_active_git_branch(self):
        """Get the name of the currently active Git branch.

        Raises:
            RuntimeError: If the active Git branch cannot be found or there's any other error.

        Returns:
            str: The name of the active Git branch."""
        try:
            # Use 'git' command to get the symbolic reference for the HEAD (current branch)
            # The command is: git symbolic-ref --short HEAD
            git_branch = subprocess.check_output(
                ["git", "symbolic-ref", "--short", "HEAD"],
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            ).strip()
            
            if not git_branch:
                raise RuntimeError("No active Git branch found.")
            
            return git_branch

        except subprocess.CalledProcessError as e:
            # Catch the error from the subprocess call
            raise RuntimeError(f"Error while trying to get active Git branch: {e}")

        except Exception as e:
            # Catch any other exceptions
            raise RuntimeError(f"Unexpected error: {e}")
        
    
    def table_exists(self, table_name): 
        """Check if a table with the given name exists in the database.

        Args:
            table_name (str): name of the table to check for existence.

        Returns:
            bool: True if table exists, False otherwise."""
        all_table_names = sa.inspect(self.postgresql_engine).get_table_names()
        return (table_name in all_table_names)
    
    def match_shortcode_to_url(self, shortcode_list, url_list):
        """Matches a list of shortcodes to a list of urls, creates a dictionary of the matches.
        
        Args:
            shortcode_list (list): A list of shortcodes to match to urls.
            url_list (list): A list of urls to match to shortcodes.
        
        Returns:
            url_shortcode_dict (dict): A dictionary of the matches between shortcodes and urls."""
        url_shortcode_dict = {}
        for shortcode in shortcode_list:
            if pd.isna(shortcode):
                    continue
            for url in url_list:
                if shortcode in url:
                    url_shortcode_dict[url] = shortcode
        return url_shortcode_dict


    def read_from_postgresql(
            self, table_name, 
            clean_date=True, date_col=None, 
            dayfirst=None, yearfirst=None, 
            format=None, errors='raise'
    ):
        """Reads a table from a PostgreSQL database table using a pscopg2 connection.
        If fails it waits 10 seconds and tries again.
        
        Args:
            table_name (str): The name of the table to read from.
            clean_date (bool, optional): Whether or not to clean the date column. Defaults to True.
            date_col (str, optional): The column name of the date column to clean. 
            dayfirst (str, optional): The day first format for date parsing. 
            yearfirst (str, optional): The year first format for date parsing.
            format (str, optional): The format for date parsing. Defaults to None.
            errors (str, optional): The behavior for date parsing errors. Defaults to 'raise'.

        Returns:
            df (pandas.DataFrame): The table data in a pandas dataframe.
        """
        date_param_error_list = []
        if clean_date == True:
            if date_col == None:
                date_param_error_list.append('date_col')
            if dayfirst == None:
                date_param_error_list.append('dayfirst')
            if yearfirst == None:
                date_param_error_list.append('yearfirst')
            if len(date_param_error_list) > 0:
                raise Exception(f"The following parameters are required to clean the date column: {date_param_error_list}")
        
        def min_max_date(df, date_col, clean_date):
            if clean_date:
                df[date_col] = pd.to_datetime(df[date_col], dayfirst=dayfirst, yearfirst=yearfirst,
                                        format=format, errors=errors)
                return f'- Min Date: {df[date_col].min()}, Max Date: {df[date_col].max()}'
            else:
                return ''

        # Connect to the PostgreSQL database
        conn = pg.connect(dbname=self.db_name, host=self.db_host,
                    port=self.db_port, user=self.db_user, password=self.db_password)
        try:
            # Try to read the table
            now = time.time()
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            time_taken = round(time.time() - now, 2)
            self.logger.logger.info(f"Read {table_name} = {time_taken}secs{min_max_date(df, date_col, clean_date)}")

        except Exception as error_message:
            # If reading the table fails, log the error and wait 10 seconds before trying again
            self.logger.logger.error(f"Read {table_name} error: {error_message}", exc_info=True)
            time.sleep(10)

            now = time.time()
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            time_taken = round(time.time() - now,2)
            self.logger.logger.info(f"Time taken to read {table_name} = {time_taken}secs{min_max_date(df, date_col, clean_date)}")

        if clean_date:
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=dayfirst, yearfirst=yearfirst,
                                        format=format, errors=errors)
        # Close the database connection
        conn.close()
        
        return df
    
    def dupes_some_cols_but_differ_in_others(
            self, df, 
            subset_cols, diff_cols,
            return_mode='only_differing_duplicates', 
            max_value_keep_col=None
    ):
        """Identifies rows that are duplicates in some columns but differ in others.

        This is useful in some cases when you want to find specific types of duplicates
        caused by specific types of errors. For example social media posts that have the
        same URL but differ in the number of impressions.

        For the "only_differing_duplicates" return_mode, the function will return the rows that are duplicates in the 
        subset_cols but differ in the diff_cols.

        For the "max_value" return_mode, the function will return the original and keep only the row with the max 
        value in the max_value_keep_col

        
        Args:
            df (DataFrame): The dataframe to identify the rows in
            subset_cols (list): The columns to check for duplicates in
            diff_cols (list): The columns to check for differences in
            return_mode (str, optional): The mode to return the duplicates in. Options are 'only_differing_duplicates' 
                or 'max_value'. Defaults to 'only_differing_duplicates'
            max_value_keep_col (str, optional): The column to use to determine which row to use to find the max value of and therefore keep
                the row item with the highest value when return_mode is 'max_value'. Defaults to None.
            
        Returns:
            df_dupes (DataFrame): Depending on the return mode, either the rows that are duplicates in the subset_cols but differ in the diff_cols or
                the original dataframe with only the row with the max value in the max_value_keep_col """
        
        if return_mode == 'only_differing_duplicates':
        # Find rows that are duplicated based on subset_columns
            duplicates = df[df.duplicated(subset=subset_cols, keep=False)]
            
            # Filter rows where any of the diff_columns have more than one unique value within each group
            result = duplicates.groupby(subset_cols).filter(lambda x: any(x[col].nunique() > 1 for col in diff_cols))
            return result
        
        elif return_mode == 'max_value':
            if not max_value_keep_col:
                raise ValueError("max_value_keep_col must be provided when return_mode is 'max_value'")
            
            # Sort the dataframe by max_value_keep_col in descending order
            df_sorted = df.sort_values(by=max_value_keep_col, ascending=False)
            
            # Drop duplicates based on subset_columns and keep the first occurrence (which has the max value due to sorting)
            result = df_sorted.drop_duplicates(subset=subset_cols, keep='first')
            
            return result

        else:
            raise ValueError("Invalid return_mode. Choose either 'only_differing_duplicates' or 'max_value'")


    def write_to_gsheet(
            self, workbook_name, 
            sheet_name, df, 
            if_exists='replace', sheet_prefix=''
    ):
        """Write a dataframe to a google sheet

        Args:
            workbook_name (str): The name of the google sheet workbook.
            sheet_name (str): The name of the sheet to write data to.
            df (pandas.DataFrame): The dataframe to be written to the google sheet.
            if_exists (str, optional): Determines the behavior when the sheet already exists, options are 'replace' or 'append'. (default='replace')
            sheet_prefix (str, optional): A prefix to be added to the sheet name. (default='')

        Returns:
            None    """
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        df['SheetUpdated'] = dt_string
        try:
            # Adding the sheet prefix to the sheet name
            sheet_name = sheet_prefix + sheet_name
            # Open the worksheet
            sheet = self.sa.open(workbook_name).worksheet(sheet_name)
            if if_exists == 'replace': 
                # Clear the sheet if if_exists is set to 'replace'
                sheet.clear()
            now = time.time()
            # Write the dataframe to the sheet
            gd.set_with_dataframe(sheet, df)
            time_taken = round(time.time() - now,2)
            self.logger.logger.info(f"Time Taken to write to google sheet {sheet_name} = {time_taken}secs")
        except Exception as error_message:
            self.logger.logger.error(error_message, exc_info=True)
            time.sleep(10)
            gd.set_with_dataframe(sheet, df)

    def read_from_gsheet(
            self, workbook_name, 
            sheet_name, clean_date=True, 
            date_col=None,
            dayfirst=None, 
            yearfirst=None, 
            format=None, errors='raise'
    ):
        """Read data from a google sheet and return it as a dataframe.

        Args:
            workbook_name (str): The name of the google sheet workbook.
            sheet_name (str): The name of the sheet to read data from.
            clean_date (bool): If true, the 'date_col' column will be converted to datetime format. (default: True)
            date_col (str): The name of the column containing the date values to be cleaned. (default: 'EnterValue')
            dayfirst (bool): Whether to interpret the first value in an ambiguous 3-integer date (e.g. 01/05/09) as the day (True) or month (False). (default: 'EnterValue')
            yearfirst (bool): Similar to 'dayfirst', but for the year. (default: 'EnterValue')
            format (str): The format of the date values. (default: None)
            errors (str): The behavior when encountering errors in the date format. (default: 'raise')

        Returns:
            df (pandas.DataFrame): The dataframe containing the data from the google sheets"""
        date_param_error_list = []
        if clean_date == True:
            if date_col == None:
                date_param_error_list.append('date_col')
            if dayfirst == None:
                date_param_error_list.append('dayfirst')
            if yearfirst == None:
                date_param_error_list.append('yearfirst')
            if len(date_param_error_list) > 0:
                raise Exception(f"The following parameters are required to clean the date column: {date_param_error_list}")
            
        try:
            spreadsheet = self.sa.open(workbook_name)
        except Exception as error_message:
            self.logger.logger.info(error_message)
            time.sleep(10)
            spreadsheet = self.sa.open(workbook_name)
        worksheet = spreadsheet.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        if clean_date:
            df[date_col] = df[date_col].str.strip()
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=dayfirst, yearfirst=yearfirst,
                                            format=format, errors=errors)
            
            print(f'Sheet = {sheet_name}, Min date = {df[date_col].min()}, Max date = {df[date_col].max()}')
        
        return df

    
    def identify_paid_or_organic(self, df):
        """Identify whether a given dataframe contains paid data"""
        paid_or_organic = 'Organic'
        # Make columns to check lower case so can work on
        # Columns that have or haven't been cleaned
        col_list = [x.lower() for x in df.columns]
        # Check to see whether "spend" or "spend_usd" is in the column list
        if 'spend' in col_list or 'spend_usd' in col_list:
            paid_or_organic = 'Paid'    
        return paid_or_organic

    def pickle_data(self, data, filename, folder="Pickled Files"):
        """Pickle data and save it to a file.
        
        Args:
            data (Object): The data to be pickled.
            filename (str): The name of the file to save the pickled data to.
            folder (str, optional): The folder to save the pickled file to. Defaults to "Pickled Files"."""

        if os.path.isdir(folder) == False:
            os.mkdir(folder)
        pickle.dump(data, open(folder + '/' + filename, "wb"))

    
    def unpickle_data(self, filename, folder ="Pickled Files"):
        """Load pickled data from a file.
        
        Args:
            filename (str): The name of the file to load the pickled data from.
            folder (str, optional): The folder where the pickled file is located. Defaults to "Pickled Files".
        
        Returns:
            Object: The unpickled data"""
        return pickle.load(open(folder + '/' + filename, "rb"))

    
    def write_json(
            self, object, 
            file_name, file_type, 
            folder="JSON Files"
    ):
        """Write a Python object to a json file.
        
        Args:
            object (Object): The Python object to be written to a json file.
            file_name (str): The name of the json file to be created.
            file_type (str): The type of the object. It must be 'DataFrame', 'List' or 'Dictionary'
            folder (str, optional): The folder to save the json file to. Defaults to "JSON Files".
        """
        if os.path.isdir(folder) == False:
            os.mkdir(folder)

        if file_type == 'DataFrame':
            object.to_json(folder + '/' + file_name + '.json', orient='split')
        elif file_type == 'List' or file_type == 'Dictionary':
            with open(f"{folder}/{file_name}.json", "w") as outfile:
                json.dump(object, outfile)
        elif file_type == 'append':
            with open(f"{folder}/{file_name}.json", "a") as outfile:
                json.dump(object, outfile)
                outfile.write("\n")
        else:
            self.logger.logger.error('JSON write error, file_type error')
    
    def read_json(
            self, file_name, 
            file_type, folder="JSON Files"
    ):
        """Read a json file and return a Python object.
        
        Args:
            file_name (str): The name of the json file to be read.
            file_type (str): The type of the object. It must be 'DataFrame', 'List' or 'Dictionary'
        
        Returns:
            Object: The object read from json file."""
        if file_type == 'DataFrame':
            return pd.read_json(f'{folder}/{file_name}.json', orient='split')
        elif file_type == 'List' or file_type == 'Dictionary':
            return json.load(open(f'{folder}/{file_name}.json'))
        elif file_type == 'append':
            with open(f"{folder}/{file_name}.json", "r") as infile:
                return [json.loads(line) for line in infile]
        else:
            self.logger.logger.error('JSON read error, file_type error')

    def columnnames_to_lowercase(self, df):
        """Change the columns in a dataframe into lowercase with spaces replaced by underscores"""
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.strip()
        return df

    def merge_match_perc(
            self, df_1, df_2, 
            left_on=None, right_on=None, 
            on=None, 
            how='left', tag="", 
            ignore_values_df2=None
    ):
        """Merges two dataframes and prints out the number of matches and the percentage of matches out of the total number of rows.
        
        Args:
            df_1 (pd.DataFrame): The first dataframe to merge
            df_2 (pd.DataFrame): The second dataframe to merge
            left_on (str): The column name to merge on in the first dataframe
            right_on (str): The column name to merge on in the second dataframe
            how (str, optional): The type of merge to perform. Defaults to 'left'.
            tag (str, optional): A tag to add to the print statement. Defaults to "".

        Returns:
            output_df (pd.DataFrame): A pandas dataframe that contains the merged data from the input dataframes."""
        
        if ignore_values_df2 == None:
            ignore_values_df2 = ['None', 'nan', '', ' ', 'none']

        if right_on != None:
            df_2['no_merge_flag'] = df_2[right_on].apply(lambda row: any(item in ignore_values_df2 for item in row), axis=1)
        elif on != None:
            df_2['no_merge_flag'] = df_2[on].apply(lambda row: any(item in ignore_values_df2 for item in row), axis=1)
            
        df_1_rows_before = df_1.shape[0] # Number of rows in df_1
        if '_merge' in df_1.columns: # If df_1 has a _merge column, drop it
            df_1 = df_1.drop('_merge', axis=1)

        output_df = pd.merge(df_1, df_2.loc[df_2['no_merge_flag']==False], left_on=left_on, right_on=right_on, how=how, on=on, indicator=True)

        # Drop the flag column in the merged dataframe
        output_df.drop(columns=['no_merge_flag'], inplace=True, errors='ignore')
        df_1_rows_after = output_df.shape[0] #number of rows in output_df after merge

        # Count the number of matches by using the "_merge" column that is created by "indicator=True"
        num_matches = output_df['_merge'].value_counts()['both']
        match_perc = round(num_matches / df_1_rows_after * 100, 1)

        self.logger.logger.info(f"{tag} df_1 has {df_1_rows_before} rows")
        self.logger.logger.info(f'{num_matches} matches, how = {how}, out of {df_1_rows_after} rows ({match_perc}%)')
        self.logger.logger.info(f'Rows after minus rows before {df_1_rows_after - df_1_rows_before}\n')
        return output_df


class Logger:
    """A class for handling logging of events."""

    def __init__(self, client_name, log_name):
        """Initialize the logger

        Args:
            client_name (str): The name of the client
            log_name (str): The name of the log file"""
        
        # Get the log directory from the environment variable
        base_log_dir = os.environ.get('LOG_DIR')

        # Create a client specific log directory
        client_log_dir = os.path.join(base_log_dir, client_name)

        # Create the client specific log directory if it doesn't exist
        os.makedirs(client_log_dir, exist_ok=True) 

        # Create the log file path
        log_file = os.path.join(client_log_dir, f'{log_name}.log') 

        # Initialize the logger
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # Check if the logger already has handlers
        if logger.hasHandlers():
            logger.handlers.clear()

        # Create a file handler for the logger
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Create a stream handler for the logger
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.INFO)

        # Create a formatter for the log messages
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        stream_formatter = logging.Formatter('%(message)s')

        # Set the formatter for the handlers
        file_handler.setFormatter(file_formatter)
        stream_handler.setFormatter(stream_formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        self.logger = logger    

class SlackNotifier:
    """A Slack notifier class to send rich-text messages using Slack's Block Kit.
    
    Attributes:
        slack_webhook_url (str): The webhook URL for the Slack channel.
        title (str): Default title for the Slack messages."""

    def __init__(self, slack_webhook_url: str, title="Data Update"):
        """Args:
            slack_webhook_url (str): The webhook URL for the Slack channel.
            title (str, optional): Default title for the Slack messages. Defaults to "Data Update"."""
        self.slack_webhook_url = slack_webhook_url
        self.title = title

    def send_slack_message(self, message: str, title: str = None, channel: str = None, **links):
        """Sends a formatted message to Slack using the provided webhook URL.
        
        Args:
            message (str): The main content of the Slack message.
            title (str, optional): The title of the Slack message. If not provided, defaults to the instance's title.
            channel (str, optional): The Slack channel ID to send the message to.
            **links (str): Key-value pairs of descriptive names and actual URLs to be included in the Slack message.
        
        Raises:
            Exception: If the request to send the Slack message fails.
            """
        if title is None:
            title = self.title

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": title
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": message
                    },
                ],
            },
            {
                "type": "divider"
            },
        ]

        for link_name, link in links.items():
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"<{link}|{link_name}>"
                }
            })

        payload = {"blocks": blocks}

        if channel:
            payload["channel"] = channel

        try:
            response = requests.post(self.slack_webhook_url, data=json.dumps(payload))
            response.raise_for_status()
        except Exception as e:
            print(f"Failed To Send Slack message: {e}")
