import pandas as pd
import email
import imaplib
import boto3
import smtplib
import requests
from typing import Optional, Dict, List, Union
import json
from io import StringIO
import time
import regex as re


class AmazonAPI:
    """A Python wrapper for interacting with the Amazon Advertising API and Amazon Marketing Cloud (AMC).

    This class provides methods for managing workflows, executing queries, and retrieving results 
    from Amazon Marketing Cloud. It simplifies API interaction by handling authentication, request 
    headers, and response parsing.

    Initialization Args:
        refresh_token (Optional[str]): The OAuth2 refresh token used to generate access tokens.
        client_id (Optional[str]): The client ID for the Amazon developer application.
        client_secret (Optional[str]): The client secret for the Amazon developer application.
        marketplace_id (Optional[str]): The marketplace ID to associate with API requests, usually a country
        instance_id (Optional[str]): The AMC instance ID to target for workflows and queries.
        advertizer_id (Optional[str]): The advertiser ID to associate with API requests.

    Attributes:
        base_url (str): The base URL for the Amazon Advertising API.
        standard_header (dict): The standard headers used for all API requests, including 
            authorization and client details.
        access_token (str): The access token generated during initialization.

    Key Methods:
        fetch_access_token: Retrieves a new access token using the provided refresh token.
        list_workflows: Fetches a list of workflows associated with the specified AMC instance.
        inspect_workflow: Retrieves details about a specific workflow.
        clean_sql_query: Formats SQL queries by removing comments and unnecessary whitespace.
        create_workflow: Creates a new workflow with a specified SQL query and workflow ID.
        update_workflow: Updates an existing workflow with a new SQL query.
        execute_saved_workflow: Executes a saved workflow with a specified time window.
        worflow_execution_status: Retrieves the current status of a workflow execution.
        download_query_results: Downloads the results of a completed workflow execution as a Pandas DataFrame.
        execute_and_download: Executes a workflow, monitors its status, and downloads the results if successful.

    Raises:
        requests.RequestException: If any API request fails.
        KeyError: If an expected response field is missing.
        ValueError: If invalid parameters are provided.

    Note:
        Ensure that the correct credentials and IDs are provided during initialization to 
        avoid authentication or access issues."""

    def __init__(self,
                 refresh_token: Optional[str] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 marketplace_id: Optional[str] = None,
                 instance_id: Optional[str] = None,
                 advertizer_id: Optional[str] = None
                 ):

        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.marketplace_id = marketplace_id
        self.instance_id = instance_id
        self.advertizer_id = advertizer_id
        self.base_url = 'https://advertising-api.amazon.com'
        self.fetch_access_token()

        self.standard_header = {
            "Authorization": f"Bearer {self.access_token}",
            "Amazon-Advertising-API-ClientID": self.client_id,
            "Amazon-Advertising-API-AdvertiserId": self.advertizer_id,
            "Amazon-Advertising-API-MarketplaceId": self.marketplace_id,
            "Content-type": "application/json"
        }

    def fetch_access_token(self):
        """Fetches a new access token using the refresh token provided during initialization.

        This method sends a POST request to Amazon's OAuth2 token endpoint to exchange the 
        refresh token for an access token. The access token is then used to authenticate 
        subsequent API requests.

        Raises:
            requests.RequestException: If the request to the token endpoint fails.
            KeyError: If the response does not contain an access token.

        Returns:
            None: The method updates the instance's `access_token` attribute.
        """
        token_url = 'https://api.amazon.com/auth/o2/token'
        body = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }

        response = requests.post(url=token_url,
                                 data=body
                                 )
        self.access_token = json.loads(response.content)['access_token']

    def list_workflows(self):
        """Retrieves a list of workflows for the specified AMC instance.

        This method sends a GET request to the AMC API to fetch all workflows associated
        with the instance ID. It handles pagination using the `nextToken` provided by 
        the API and filters workflows that contain SQL queries.

        Returns:
            List[Dict[str, str]]: A list of dictionaries containing `sqlQuery` and `workflowId` 
            for each workflow that includes a SQL query. Each SQL query is cleaned to remove 
            newlines.

        Raises:
            requests.RequestException: If the API request fails.
            KeyError: If the response does not contain expected fields like `workflows` or `nextToken`.

        Example:
            >>> workflows = api.list_workflows()
            >>> print(workflows)
            [{'sqlQuery': 'SELECT * FROM ...', 'workflowId': 'workflow_123'}, ...]"""

        next_token = None
        workflows = []
        params = None

        while True:

            url = self.base_url + \
                f'/amc/reporting/{self.instance_id}/workflows'
            print(url)

            if next_token:
                params = {'nextToken': next_token}

            response = requests.get(
                url=url, headers=self.standard_header, params=params)

            print(f"Status code = {response.status_code}")

            print(response.text)

            workflows.extend(response.json()['workflows'])

            next_token = response.json().get('nextToken')

            if not next_token:
                break

            time.sleep(2)

        workflows = [x for x in workflows if x.get('sqlQuery') != None]

        workflows = [{'sqlQuery': x['sqlQuery'].replace(
            "\n", ""), 'workflowId': x['workflowId']} for x in workflows]

        return workflows

    def inspect_workflow(self, workflow_id: str):
        """Retrieves details of a specific workflow by its ID.

        Sends a GET request to the AMC API to fetch metadata and details about a
        particular workflow identified by `workflow_id`.

        Args:
            workflow_id (str): The unique identifier of the workflow to inspect.

        Returns:
            None: The response from the API is printed to the console.

        Raises:
            requests.RequestException: If the API request fails.
            KeyError: If the response does not contain expected fields."""

        url = self.base_url + \
            f'/amc/reporting/{self.instance_id}/workflows/{workflow_id}'

        response = requests.get(url=url, headers=self.standard_header)

        print(response.text)
    
    def clean_sql_query(self, sql_query: str):
        """Cleans a SQL query by removing comments, newlines, and unnecessary whitespace.

        This method ensures that the SQL query is in a single-line format suitable for
        API requests. It removes single-line comments (starting with `--`), multi-line
        comments (enclosed in `/* */`), newlines, and tabs.

        Args:
            sql_query (str): The raw SQL query string to be cleaned.

        Returns:
            str: A cleaned SQL query formatted as a single line without comments or extra whitespace.

        Example:
            >>> query = '''
            ... SELECT * FROM table -- This is a comment
            ... WHERE column = 1 /* Multi-line
            ... comment */
            ... '''
            >>> clean_query = api.clean_sql_query(query)
            >>> print(clean_query)
            SELECT * FROM table WHERE column = 1
        """

        # Use re.sub() to replace the single line comments with an empty string
        # The flags=re.MULTILINE make the $ match end of lines in a multi-line string
        # sql_query = re.sub(r"--.*?$", "", sql_query, flags=re.MULTILINE)
        sql_query = re.sub(r"--[^\n]*", "", sql_query)

        # Remove multi-line sql comments
        sql_query = re.sub(r'/\*.*?\*/', '', sql_query, flags=re.DOTALL)

        return sql_query.replace("\n", " ").replace("\t", " ").strip()


    def create_workflow(self,
                        sql_query: str,
                        workflow_id: str
                        ):
        """Creates a new workflow in AMC with the specified SQL query and workflow ID.

        This method sends a POST request to the AMC API to create a workflow. The SQL
        query is cleaned to ensure it is properly formatted for the API, and the workflow
        is assigned a unique ID.

        Args:
            sql_query (str): The SQL query defining the workflow.
            workflow_id (str): The unique identifier for the new workflow.

        Returns:
            None: The response from the API is printed to the console.

        Raises:
            requests.RequestException: If the API request fails.
            KeyError: If the response does not contain expected fields.

        Example:
            >>> sql_query = "SELECT * FROM table WHERE column = 1"
            >>> api.create_workflow(sql_query, "workflow_123")
            Status code = 201
            {"workflowId": "workflow_123", "status": "CREATED", ...}
        """

        sql_query = self.clean_sql_query(sql_query)

        print(sql_query)

        body = {
            # "distinctUserCountColumn": "du_count",
            "sqlQuery": sql_query,
            "workflowId": workflow_id
        }

        url = self.base_url + f'/amc/reporting/{self.instance_id}/workflows'
        print(url)

        response = requests.post(
            url=url, json=body, headers=self.standard_header)

        print(f"Status code = {response.status_code}")

        print(response.text)
    
    def update_workflow(self,
                        workflow_id: str,
                        sql_query: str
                        ):
        """Updates an existing workflow in AMC with a new SQL query.

        This method sends a PUT request to the AMC API to update the SQL query of an
        existing workflow identified by `workflow_id`. The SQL query is cleaned to ensure
        it is properly formatted for the API.

        Args:
            workflow_id (str): The unique identifier of the workflow to update.
            sql_query (str): The new SQL query to assign to the workflow.

        Returns:
            None: The response from the API is printed to the console.

        Raises:
            requests.RequestException: If the API request fails.
            KeyError: If the response does not contain expected fields.

        Example:
            >>> sql_query = "SELECT * FROM table WHERE column = 2"
            >>> api.update_workflow("workflow_123", sql_query)
            Status code = 200
            {"workflowId": "workflow_123", "status": "UPDATED", ...}
        """

        sql_query = self.clean_sql_query(sql_query)

        body = {
            # "distinctUserCountColumn": "du_count",
            "sqlQuery": sql_query,
            "workflowId": workflow_id
        }

        url = self.base_url + f'/amc/reporting/{self.instance_id}/workflows/{workflow_id}'
        print(url)

        response = requests.put(
            url=url, json=body, headers=self.standard_header)
        
        print(f"Status code = {response.status_code}")

        print(response.text)

    def execute_saved_workflow(self,
                               workflow_id: str,
                               time_window_start: Optional[str] = None,
                               time_window_end: Optional[str] = None,
                               time_window_type: Optional[str] = None,
                               time_window_time_zone: Optional[str] = None
                               ):

        """Executes a saved workflow with the specified time window and returns the execution ID.

        Sends a POST request to the AMC API to execute the workflow identified by `workflow_id`. 
        The time window can be specified explicitly or by using predefined time window types.

        Args:
            workflow_id (str): The unique identifier of the workflow to execute.
            time_window_start (Optional[str]): The start of the time window (ISO 8601 format), 
                required if `time_window_type` is `EXPLICIT`.
            time_window_end (Optional[str]): The end of the time window (ISO 8601 format), 
                required if `time_window_type` is `EXPLICIT`.
            time_window_type (Optional[str]): The type of time window to use. If not provided, 
                defaults to `MOST_RECENT_DAY`. Valid options are:
                    - `EXPLICIT`: The start and end of the time window must be explicitly 
                      provided in the request.
                    - `MOST_RECENT_DAY`: The most recent 1-day window for which data is available, 
                      aligned to day boundaries.
                    - `MOST_RECENT_WEEK`: The most recent 1-week window for which data is available, 
                      aligned to day boundaries.
                    - `CURRENT_MONTH`: From the start of the current month to the most recent time 
                      for which data is available.
                    - `PREVIOUS_MONTH`: The entire previous month.
            time_window_time_zone (Optional[str]): The time zone for the time window, e.g. 'America/New_York'. 
                If not provided, defaults to the instance's configured time zone. 

        Returns:
            str: The workflow execution ID.

        Raises:
            requests.RequestException: If the API request fails.
            KeyError: If the response does not contain the expected fields.

        Example:
            >>> workflow_id = "workflow_123"
            >>> execution_id = api.execute_saved_workflow(
            ...     workflow_id, 
            ...     time_window_type="MOST_RECENT_WEEK"
            ... )
            >>> print(execution_id)
            workflowExecution_456"""

        body = {"workflowId": workflow_id,
                "timeWindowStart": time_window_start,
                "timeWindowEnd": time_window_end,
                "timeWindowType": time_window_type,
                "timeWindowTimeZone": time_window_time_zone
        }

        url = self.base_url + \
            f'/amc/reporting/{self.instance_id}/workflowExecutions'

        response = requests.post(
            url=url, json=body, headers=self.standard_header)

        print(f"Status code = {response.status_code}")

        print(response.text)

        return response.json()['workflowExecutionId']

    def worflow_execution_status(self,
                                 workflow_execution_id: str
                                 ):

        """Retrieves the current status of a workflow execution.

        Sends a GET request to the AMC API to fetch the status of a workflow execution
        identified by `workflow_execution_id`.

        Args:
            workflow_execution_id (str): The unique identifier of the workflow execution.

        Returns:
            str: The status of the workflow execution. 


        Raises:
            requests.RequestException: If the API request fails.
            KeyError: If the response does not contain the expected `status` field.

        Example:
            >>> execution_id = "workflowExecution_456"
            >>> status = api.worflow_execution_status(execution_id)
            >>> print(status)
            "RUNNING"
        """


        url = self.base_url + \
            f'/amc/reporting/{self.instance_id}/workflowExecutions/{workflow_execution_id}'

        response = requests.get(url=url, headers=self.standard_header)

        print(response.json())

        return response.json()['status']

    def download_query_results(self,
                               workflow_execution_id: str,
                               skiprows: Optional[int] = None,
                               quoting: int = 0
                               ):
        """Downloads query results for a completed workflow execution and returns them as a Pandas DataFrame.

        Sends a GET request to retrieve the download URL for the workflow execution results, 
        then fetches and parses the results into a DataFrame.

        Args:
            workflow_execution_id (str): The unique identifier of the workflow execution.
            skiprows (Optional[int]): The number of rows to skip at the beginning of the CSV file. 
                Defaults to None.
            quoting (int): Controls how quotes in the CSV data are handled. Uses `csv` module constants:
                - `csv.QUOTE_MINIMAL` (0): Quote fields containing special characters.
                - `csv.QUOTE_ALL` (1): Quote all fields.
                - `csv.QUOTE_NONNUMERIC` (2): Quote all non-numeric fields.
                - `csv.QUOTE_NONE` (3): Do not quote fields. Defaults to 0.

        Returns:
            pd.DataFrame: A DataFrame containing the query results.

        Raises:
            requests.RequestException: If the API request fails or the download URL is unreachable.
            ValueError: If the download fails with a non-200 status code.
            pd.errors.ParserError: If Pandas encounters an issue parsing the CSV data.
        """

        url = self.base_url + \
            f'/amc/reporting/{self.instance_id}/workflowExecutions/{workflow_execution_id}/downloadUrls'

        response = requests.get(url=url, headers=self.standard_header)
        print(response.text)

        download_url = response.json()['downloadUrls'][0]

        try:
            response = requests.get(download_url)

            if response.status_code == 200:
                data = StringIO(response.content.decode('utf-8'))
                df = pd.read_csv(data, skiprows=skiprows, quoting=quoting)
            else:
                raise ValueError(
                    f"Failed to download: Status code {response.status_code}")

            print(f'Number of rows in the dataframe is {df.shape[0]}')

            return df

        except requests.RequestException as e:
            print(f"Request failed: {e}")

        except pd.errors.ParserError as e:
            print(f"Pandas failed to parse the CSV data: {e}")
            raise

    def execute_and_download(self,
                             workflow_id: str,
                             time_window_start: Optional[str] = None,
                             time_window_end: Optional[str] = None,
                             time_window_type: Optional[str] = None,
                             time_window_time_zone: Optional[str] = None
                             ):
        """Executes a saved workflow, monitors its status, and downloads the results if successful.

        This method combines workflow execution, status monitoring, and result retrieval. 
        It retries the status check with an exponential backoff strategy until the workflow 
        either succeeds or fails, or the maximum number of attempts is reached.

        Args:
            workflow_id (str): The unique identifier of the workflow to execute.
            time_window_start (Optional[str]): The start of the time window (ISO 8601 format), 
                required if `time_window_type` is `EXPLICIT`.
            time_window_end (Optional[str]): The end of the time window (ISO 8601 format), 
                required if `time_window_type` is `EXPLICIT`.
            time_window_type (Optional[str]): The type of time window to use. If not provided, 
                defaults to `MOST_RECENT_DAY`. Valid options are:
                    - `EXPLICIT`: The start and end of the time window must be explicitly provided.
                    - `MOST_RECENT_DAY`: The most recent 1-day window for which data is available.
                    - `MOST_RECENT_WEEK`: The most recent 1-week window for which data is available.
                    - `CURRENT_MONTH`: From the start of the current month to the most recent time.
                    - `PREVIOUS_MONTH`: The entire previous month.
            time_window_time_zone (Optional[str]): The time zone for the time window, e.g. 'America/New_York'. 
                If not provided, defaults to the instance's configured time zone.

        Returns:
            pd.DataFrame: A DataFrame containing the query results if the workflow succeeds.

        Raises:
            Exception: If the workflow execution fails or does not succeed within the maximum 
                number of retries.
            requests.RequestException: If any API request fails during execution or result download.
            ValueError: If the result download fails with a non-200 status code.
            pd.errors.ParserError: If Pandas encounters an issue parsing the CSV data.
        """

        workflow_execution_id = self.execute_saved_workflow(workflow_id,
                                                            time_window_start,
                                                            time_window_end,
                                                            time_window_type,
                                                            time_window_time_zone)

        status = self.worflow_execution_status(workflow_execution_id)

        attempt = 0
        max_attempts = 14
        sleep_time = 2

        while status not in ["SUCCEEDED", "FAILED"] and attempt < max_attempts:

            time.sleep(sleep_time)
            status = self.worflow_execution_status(workflow_execution_id)
            print(
                f"Current status: {status}, checking again in {sleep_time} seconds...")

            sleep_time *= 2  # Double the wait time for the next iteration up to a maximum
            attempt += 1

        if status == "SUCCEEDED":
            print("Workflow completed successfully. Proceeding to download.")
            return self.download_query_results(workflow_execution_id)
        else:
            raise Exception(
                f"Workflow did not succeed, current status: {status}")