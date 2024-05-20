#%%
import pandas as pd
from snowflake.sqlalchemy import URL
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import time
import math
from sqlalchemy import create_engine
#%%
class Snowflake():
    """A class for connecting to Snowflake wrapping around the snowflake python connector.

    https://docs.snowflake.com/en/developer-guide/snowpark/python/index
    
    The additional features include reading large tables in chunks, and having pauses and retries if a chunk read is unsuccessful."""
    
    def __init__(self, connection_params_dict) -> None:
        """Initialise the snowflake connection with a dictionary of connection parameters.

        Log File is created in the LOG_DIR environment variable with a subdirectory with the name of the client_name
        
        Args:
            connection_params_dict (dict): Dictionary of connection parameters
            client_name (str): Name of the client, this is used to create a folder in the LOG_DIR environment variable
        
        Returns:
            None"""
        
        self.connection_params_dict = connection_params_dict

        self.user = self.connection_params_dict['user']
        self.role = self.connection_params_dict['role']
        self.warehouse = self.connection_params_dict['warehouse']
        self.database = self.connection_params_dict['database']
        self.schema = self.connection_params_dict['schema']
        self.account = self.connection_params_dict['account']
        self.password = self.connection_params_dict['password']
    
    def read_snowflake_to_df(
            self, table_name, 
            schema=None, database=None,
            chunk_size=200000):
        """Function to read Snowflake table using SQLAlchemy
        
        URL stands for Uniform Resource Locator. It is a reference (an address) to a resource on the Internet.
        
        Args:
            table_name (str): Name of the table to read
            schema (str, optional): Name of the schema to read from. Defaults to the schema specified on class initialisation.
            chunk_size (int, optional): Size of the data chunks to read in a single batch. Defaults to 200000.
        
        Returns:
            df: Pandas dataframe of the data read from Snowflake"""

        if schema == None:
            schema = self.schema
        if database == None:
            database = self.database

        with snowflake.connector.connect(   
            user = self.user,
            password = self.password,
            account = self.account,
            warehouse = self.warehouse,
            database = database,
            schema = schema,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                num_rows = cur.fetchone()[0]
                print(f"Total rows in {table_name}: {num_rows}")

                num_chunks = math.ceil(num_rows / chunk_size)
                print(f"Fetching {num_rows} rows in {num_chunks} chunks of {chunk_size} rows each")
            
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table_name}")
                
                # Initialize an empty DataFrame to hold the results
                df_total = pd.DataFrame()

                for chunk_index in range(num_chunks):
                    rows = cur.fetchmany(chunk_size)
                    if not rows:
                        break

                    df_chunk = pd.DataFrame(rows, columns=[x[0] for x in cur.description])
                    df_total = pd.concat([df_total, df_chunk], ignore_index=True)

                    # For now, we'll just print the size of each chunk
                    print(f"Fetched chunk {chunk_index + 1} of {num_chunks}, size: {len(df_chunk)} rows")

        return df_total

    
    def write_df_to_snowflake(
        self, df, table_name, 
        database=None, schema=None,
        auto_create_table=False, 
        overwrite=False, chunk_size=20000):
        '''Function to write Pandas dataframe to Snowflake table.

        Truncates (if it exists) or creates new table and inserts the new data into the selcted table.

        This function is based on the write_pandas function from the snowflake-connector-python package
        but just adds some redundancy and retries if the connection fails.

        Their documentation can be found here, but is incomplete as it doesn't include the overwrite parameter.


        https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#write_pandas
        
        Args:
            df (dataframe): Pandas dataframe to write to Snowflake
            table_name (str): Name of the table to write to
            database (str, optional): Name of the database to write the table to. Defaults to the database specified on class initialisation.
            schema (str, optional): Name of the schema to write the table to. Defaults to the schema specified on class initialisation.
            auto_create_table (bool, optional): If True, creates the table if it does not exist. Defaults to False.
            overwrite (bool, optional): If True, overwrites the table if it exists, else if False the df is appended to current table. Defaults to False.
            chunk_size (int, optional): Size of the data chunks to write in a single batch. Defaults to 200000.
        
        Returns:
            None'''
        
        if schema == None:
            schema = self.schema
        if database == None:
            database = self.database
        
        with snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=schema,
        ) as conn:
            print(f"Connected to Snowflake")
            try:
                now = time.time()
                success, nchunks, nrows, _ = write_pandas(
                    conn, df, 
                    table_name, 
                    parallel=8,
                    schema=schema,
                    database=database,
                    auto_create_table=auto_create_table,
                    overwrite=overwrite,
                    chunk_size=chunk_size
                )
                time_taken = round(time.time() - now,2)
                print(f"Sent Data to {table_name}, time taken: {time_taken} seconds")
                
            except Exception as error_message:
                print(f"Connection error {error_message}")
                time.sleep(10)  # wait for 10 seconds then try again
                try:
                    now = time.time()
                    success, nchunks, nrows, _ = write_pandas(conn,df, table_name, parallel=8,schema=schema,database=database,
                                            auto_create_table=auto_create_table,overwrite=overwrite,chunk_size=chunk_size)
                    time_taken = round(time.time() - now,2)
                    print(f"Sent Data to {table_name} time taken: {time_taken} seconds")
                    
                except Exception as error_message:
                    print("Connection failed again")
                    print(f'{table_name} error: ' + str(error_message))
                    raise Exception(error_message)

    def drop_table(self, table_name, database=None, schema=None):
            '''Function to drop Snowflake table
            
            Args:
                table_name (str): Name of the table to drop
                database (str, optional): Name of the database to drop the table from. Defaults to the database specified on class initialisation.
                schema (str, optional): Name of the schema to drop the table from. Defaults to the schema specified on class initialisation.
            
            Returns:
                Message that the table has been dropped'''
            
            # Set default values for database and schema if not provided
            if schema == None:
                schema = self.schema
            if database == None:
                database = self.database
            
            # Create connection to Snowflake
            with snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=schema,
            ) as conn:
            
                # Create cursor
                with conn.cursor() as cur:
                    
                    # Drop table
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                    
                    # Commit changes
                    conn.commit()
                    
            return f"Table {table_name} has been dropped."
    
    def send_sql_query(self, sql_query, database=None, schema=None):
        """Executes a SQL query on a Snowflake database using the specified schema and database.

        This function creates a connection to a Snowflake database using provided credentials,
        executes a given SQL query, and commits the changes. It defaults to the instance's database
        and schema if none are provided.

        Args:
            sql_query (str): The SQL query to execute. This could be any valid SQL command.
            database (str, optional): The database to connect to. Defaults to the instance's database if None.
            schema (str, optional): The schema to use within the database. Defaults to the instance's schema if None.

        Raises:
            snowflake.connector.Error: If there is an issue with the connection or the SQL execution.
        """
        
        # Set default values for database and schema if not provided
        if schema == None:
            schema = self.schema
        if database == None:
            database = self.database
        
        # Create connection to Snowflake
        with snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=schema,
        ) as conn:
            
            # Create cursor and execute SQL query
            with conn.cursor() as cur:
                cur.execute(sql_query)

                # Check if the SQL query was a SELECT statement and fetch results
                if sql_query.strip().upper().startswith('SELECT'):
                    result = cur.fetch_pandas_all()
                else:
                    # If not a SELECT query, commit changes and return None
                    conn.commit()
                    return None

        return result


