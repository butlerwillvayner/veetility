import pandas as pd
import email
import imaplib
import boto3
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import io
import os
import requests
import regex as re
from io import StringIO
from datetime import datetime
from typing import Optional, Dict, List, Union
today = datetime.today().date()

class VEEmail:
    """A class for handling email operations for receiving and sending data using the imaplib library
    
    The class provides a set of methods to interact with an google email server
    
    Attributes:
        username (str): The email address of the email account
        password (str): The password of the email account
        con (imaplib.IMAP4_SSL): The IMAP client connection object.
        search_string (str): The string written in the IMAP search criteria format
    
    Methods:
        - search_emails(): Searches for emails based on the sender's email address, substrings within a subject, and/or whether they were sent today.
        - get_msg_object(): Return the message object given the email ID, from which you can get details on the properties of the email
        - get_email_body(): Return the email body (i.e. the text) given the email ID
        - does_message_have_attachment(): Check if the message object contains an attachment that is either a CSV or Excel file.
        - csv_from_url_to_df(): Fetches a CSV file from a specified URL and loads it into a Pandas DataFrame.
        - extract_url_from_body(): Extracts a single URL from the given text body.
        - attachments_to_df(): Retrieves an attachment from an email, converts it to a DataFrame, and returns it.
        - parse_csv(): Parses the provided CSV content into a pandas DataFrame, identifying the header row based on key columns.
    
    Note:
        This class requires the imaplib library. Ensure you have this library installed and accessible in your Python environment."""
    
    def __init__(self, 
                 username: str, 
                 password: str, 
                 debug_level: int=2):
        """Initializes the email client with a connection to an IMAP server, specifically targeting Gmail.

        This method establishes a secure IMAP connection to the Gmail server using the provided username and password. It sets the debug level for IMAP operations and selects the "[Gmail]/All Mail" folder to ensure all emails across different inboxes are accessible.

        Args:
            username (str): The username (email address) for the Gmail account.
            password (str): The password for the Gmail account.
            debug_level (int, optional): The debug level for the IMAP client to control the verbosity of debug output. Defaults to 2.

        Attributes:
            con (imaplib.IMAP4_SSL): The IMAP connection object.
            search_string (str): A string used for specifying search criteria in IMAP queries. Initialized as an empty string."""

        imap_server = 'imap.gmail.com'
        con = imaplib.IMAP4_SSL(imap_server)
        con.login(username, password)
        imaplib.debug = debug_level

        # This line is crucial for making sure all email inboxes are searched
        con.select('"[Gmail]/All Mail"') 
        self.con = con
        self.username = username
        self.password = password
        self.search_string = ""
    
    def search_emails(self, 
            from_email=None, 
            substrings_in_subject=None, 
            exclude_subject_substring=None, 
            send_date=None,
            sent_today=None
    ):
        """ Searches for emails based on the sender's email address, substrings within a subject, and/or whether they were sent today.

        This method constructs a search string based on the provided parameters and uses it to search through emails. 
        It returns a list of email IDs that meet the specified criteria.
        """
        if send_date is not None and sent_today:
            raise ValueError("Cannot use both 'send_date' and 'sent_today' parameters simultaneously.")
        
        search_string = ''

        if from_email != None:
            search_string += f'(FROM "{from_email}")'
        
        if substrings_in_subject != None:
            for substring in substrings_in_subject:
                search_string += f'(SUBJECT "{substring}")'
        
        if exclude_subject_substring != None:
            search_string += f'(NOT SUBJECT "{exclude_subject_substring}")'
        
        if send_date != None:
            search_string += f'(SENTON "{send_date}")'
        
        if sent_today:
            search_string += f'(SENTON "{today.strftime("%d-%b-%Y")}")'
        
        # You have to have space in between the different bracketed search items 
        # but you can't have a space at the beginning of your search query
        search_string = search_string.replace(")(", ") (")

        print(f'Search String = {search_string}')
        self.search_string = search_string

        status, email_ids = self.con.search(None, self.search_string)

        if status != 'OK':
            print(f"Status = {status}")
            return []
        
        # Sort IDs numerically to get the most recent emails first
        email_ids = sorted(email_ids[0].split(), key=int, reverse=True)

        return email_ids
    
    

    def get_msg_object(self, 
                       email_id: bytes):
        """Return the message object given the email ID, from which you can get details on what the email contains
        
        Returns a dictionary object containing information such as 'Delivered-To','Received',
        'Received-SPF', 'Authentication-Results', 'Content-Type', 'MIME-Version', 'Subject',
        'From', 'To', 'Message-ID', 'Date','Feedback-ID'
        
        Args:
            email_id (bytes): email_id in byte format
        
        Returns:
            msg object (dict): Dictionary of attributes of the email 
        """
        _, email_data = self.con.fetch(email_id, '(RFC822)')

        try:
            raw_email = email_data[0][1]
            msg = email.message_from_bytes(raw_email)
        except:
            print("Email Data Invalid")

        print(f"Subject: {msg['Subject']} - {msg['Date']}")
        print(f"Content Type = {msg['Content-Type']}")

        return msg

    
    def get_email_body(self, 
                       email_id: bytes):
        ''' Return the email body (i.e. the text) given the email ID
        Args:
            email_id: Email ID in byte format

        Returns:
            email_body: string of the main text message of the email
        '''
        
        email_msg = self.get_msg_object(email_id)

        if email_msg.is_multipart():
            for part in email_msg.walk():
                content_type = part.get_content_type()
                if content_type in ["text/plain", "text/html"]:
                    return part.get_payload(decode=True).decode()

        else:
            return email_msg.get_payload(decode=True).decode()
    
    def does_message_have_attachment(self, 
                                     msg: str):
        """Check if the message object contains an attachment that is either a CSV or Excel file.

        This method iterates through each part of the given email message object. 
        It checks if the part is an attachment by examining the content type and 
        presence of 'Content-Disposition'. If the part is an attachment, the method 
        further checks if the file extension is one of the following: '.csv', '.xls', 
        '.xlsx', '.xlsm'. If such an attachment is found, the method returns True.

        Args:
            msg (email.message.Message): The email message to be checked.

        Returns:
            bool: True if the message contains an attachment that is a CSV or Excel file,
                otherwise False."""
        
        for part in msg.walk():
            # Check if the part is an attachment
            if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
                continue

            # Get the filename of the attachment
            file_name = part.get_filename()
            if file_name:
                # Check if the file extension indicates a CSV or Excel file
                extension = os.path.splitext(file_name)[1].lower()
                if extension in ['.csv', '.xls', '.xlsx', '.xlsm']:
                    return True
        return False


    def csv_from_url_to_df(self, 
                        url: str,
                        skiprows: Optional[int]=None, 
                        quoting: int=0):
        """Fetches a CSV file from a specified URL and loads it into a Pandas DataFrame.

        Args:
            url (str): The URL of the CSV file to download.
            skiprows(int): The number of rows to skip, incase there is header information on the file
            quoting()

        Returns:
            DataFrame: A Pandas DataFrame containing the data from the CSV file.

        Raises:
            ValueError: If the request does not return a successful status code.
            Exception: If Pandas is unable to read the CSV data."""
        
        try: 
            response = requests.get(url)

            # Ensure the request was successful
            if response.status_code == 200:
                data = StringIO(response.content.decode('utf-8'))
                df = pd.read_csv(data, skiprows=skiprows, quoting=quoting)
            else:
                raise ValueError(f"Failed to download: Status code {response.status_code}")
            
            print(f'Number of rows in the dataframe is {df.shape[0]}')
            return df
        
        except requests.RequestException as e:
            print(f"Request failed: {e}")
        
        except pd.errors.ParserError as e:
            print(f"Pandas failed to parse the CSV data: {e}")
            raise

    def extract_url_from_body(self, 
                            body: str,
                            base_url: Optional[str]=None):
        """Extracts a single URL from the given text body.

        This function searches the provided text for URLs. If a base_url is provided, it specifically looks for URLs that start with this base. It is designed to return a single URL; if no URL or more than one URL is found, it raises a ValueError.

        Args:
            body (str): The text body from which to extract the URL.
            base_url (str, optional): The base URL to filter the URLs in the text. If None, the function searches for any URL. Defaults to None.

        Returns:
            str: The extracted URL if exactly one URL is found.

        Raises:
            ValueError: If no URLs or more than one URL are found in the body.
        
        Examples:
            >>> extract_url_from_body("Check out this website: https://example.com", "https://example.com")
            'https://example.com'
            >>> extract_url_from_body("No URLs here", "https://example.com")
            ValueError: No URL found in the email content.
            >>> extract_url_from_body("Multiple URLs: https://example.com, https://example.org")
            ValueError: More than one URL found in the email content."""

        if base_url == None:
            pattern = r'https?://[^\s]+'
        else:
            pattern = f'{base_url}[^\s]+'

        urls = re.findall(pattern, body)

        # Check the number of URLs found
        if len(urls) == 1:
            return urls[0]
        elif len(urls) == 0:
            raise ValueError("No URL found in the email content.")
        else:
            raise ValueError("More than one URL found in the email content.")



    def attachments_to_df(self, 
                        email_id: bytes, 
                        attachment_dir: str='', 
                        key_columns: Optional[List[str]]=None,
                        skiprows: Optional[Union[int, List[int]]]=None
    ):
        """Retrieves an attachment from an email, converts it to a DataFrame, and returns it.

        This method gets an email message by its ID, then iterates through each part of the message.
        It checks for attachments with either 'application/octet-stream' or 'application' content types,
        and 'text/csv' content type. For attachments with 'application' content types, the method writes
        the file to a directory, reads its content into a DataFrame, deletes the file, and returns the DataFrame.
        For 'text/csv' content type, it directly reads the content into a DataFrame and returns it. The DataFrame
        is parsed based on key columns if provided, otherwise it uses default key columns: ['Day', 'Media Owner',
        'Venue Type', 'Advertiser'].

        Args:
            email_id (str): The ID of the email to retrieve the attachment from, encoded in bytes
            attachment_dir (str, optional): The directory where attachments will be temporarily saved. 
                                        Defaults to an empty string, which indicates the current directory.
            key_columns (list of str, optional): A list of column names to be used as key columns for parsing the CSV. 
                                                Defaults to ['Day', 'Media Owner', 'Venue Type', 'Advertiser'].

        Returns:
            pandas.DataFrame: A DataFrame containing the data from the email attachment."""

        if key_columns == None:
            key_columns = ['Day', 'Media Owner', 'Venue Type', 'Advertiser']

        msg = self.get_msg_object(email_id)

        
        for part in msg.walk():

            print(f'part.get_content_maintype() {part.get_content_maintype()}')
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
            
            # I think this works when the part content type = 'application'
            if part.get_content_maintype() in ['application/octet-stream', 'application']:
                print("Attachment is in content type 'application/octet-stream'")
                fileName = part.get_filename()
                print(f'Part Content Type {part.get_content_type()}')
                print(f"fileName = {fileName}")

                if bool(fileName):
                    csv_file_path = os.path.join(attachment_dir, fileName)
                    with open(csv_file_path,'wb') as f:
                        f.write(part.get_payload(decode=True))

                print(f'csv_file_path = {csv_file_path}')
                csv_content = open(csv_file_path).read()
                df = self.parse_csv(csv_content, key_columns)
                print(f"Number of rows of dataset = {df.shape[0]}")
                os.remove(csv_file_path)

                return df

            if part.get_content_type() == 'text/csv':
                print("Attachment is in content type 'text/csv'")
                attachment = part.get_payload(decode=True)
                csv_data = attachment.decode('utf-8').splitlines()
                df = pd.read_csv(io.StringIO('\n'.join(csv_data)), skiprows=skiprows, sep=',')

                return df
            

    def parse_csv(self, 
                csv_content: str,
                key_columns: Optional[List[str]]):
        """Parses the provided CSV content into a pandas DataFrame, identifying the header row based on key columns.

        This function processes a string representation of CSV data. It identifies the header row by searching for specified key columns. If the key columns are not provided, it defaults to a predefined set of columns.

        Args:
            csv_content (str): The CSV data as a string.
            key_columns (list of str, optional): A list of column names to identify the header row. Defaults to ['Day', 'Media Owner', 'Venue Type', 'Advertiser'].

        Returns:
            DataFrame: A pandas DataFrame containing the parsed CSV data starting from the identified header row.

        Raises:
            ValueError: If the header row with the specified key columns is not found (if the corresponding code block is uncommented).

        Note:
            The function currently does not raise an error if the header row is not found. To enable this functionality, uncomment the relevant code block."""
        
        if key_columns == None:
            key_columns = ['Day', 'Media Owner', 'Venue Type', 'Advertiser']

        # Split the CSV content into individual lines using the newline character.
        lines = csv_content.split('\n')

        # Initialize a variable to store the index of the header row.
        header_row_index = None

        # Iterate over each line in the CSV content.
        for i, line in enumerate(lines):
            print(f'line = {line}')
            # Check if all the key columns are present in the current line.
            # This is done by checking if each key column is a substring of the line.
            if all(key_column in line for key_column in key_columns):
                print(f'row index found')
                # If all key columns are found, set the current line index as the header row index.
                header_row_index = i
                # Break the loop as we have found the header row.
                break

        # Uncomment the following lines if you want to raise an error when the header row is not found.
        # if header_row_index is None:
        #     raise ValueError("Header row with key columns not found.")

        # Extract the CSV content starting from the header row.
        csv_content_from_header = '\n'.join(lines[header_row_index:])

        # Parse the extracted CSV content into a pandas DataFrame.
        # StringIO is used to convert the string content into a file-like object for pd.read_csv.
        parsed_df = pd.read_csv(io.StringIO(csv_content_from_header))

        # Return the parsed DataFrame.
        return parsed_df

    def send_email(self, to_email: str, subject: str, body: str, attachment_path: Optional[str] = None):
        """Sends an email with an optional attachment.

        Args:
            to_email (str): The recipient's email address.
            subject (str): The subject of the email.
            body (str): The body of the email.
            attachment_path (str, optional): The file path of the
            Raises: Exception: If there is an issue with sending the email.
        """
        from_email = self.username

        # Create the MIME message object
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject

        # Attach the body text
        msg.attach(MIMEText(body, 'plain'))

        # Attach the file if provided
        if attachment_path:
            attachment = MIMEBase('application', 'octet-stream')
            with open(attachment_path, 'rb') as file:
                attachment.set_payload(file.read())
            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
            msg.attach(attachment)

        # Establish a secure session with the server and send the email
        try:
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(from_email, self.password)
            server.sendmail(from_email, to_email, msg.as_string())
            server.quit()
            print("Email sent successfully!")
        except Exception as e:
            print(f"Failed to send email: {e}")
            raise