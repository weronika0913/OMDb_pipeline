"""
Module responsible for user authentication and database management.

This module contains classes for:
- Handling API authentication by retrieving tokens from environment variables.
- Managing DuckDB database connections and operations such as executing SQL,
  loading CSV files, and inserting data from pandas DataFrames.
"""

import os
from dotenv import load_dotenv
import duckdb


class BaseApiAuth:
    """
    BaseApiAuth provides a base interface for API-based authentication

    Arg:
        url: base URL of the API
        endpoint: the url endpoint from which data will be extracted

    """

    def __init__(self, url: str = "http://www.omdbapi.com/"):
        self.full_url = url

    def get_token(self, token_name: str = "API_KEY") -> str:
        """
        Retrive token details from env file

        Arg:
            token_name: name of variable where Api Key is stored

        Raises:
            ValueError: if the api_key is none

        Return:
            str
        """
        load_dotenv()
        api_key = os.getenv(token_name)
        if api_key is None:
            raise ValueError(f"{token_name} environment variable is not set")
        return api_key


class DatabaseManager:
    def __init__(self, dbname: str = "Movies.db"):
        """
        Initializes a connection to the DuckDB database.

        Args:
            dbname (str): The database file name.
        """
        self.dbname = dbname
        self.conn = duckdb.connect(self.dbname)

    def execute_sql(self, sql: str, success_msg: str = None):
        """
        Executes a SQL command that modifies data (INSERT, UPDATE, DELETE).

        Args:
            sql (str): The SQL query to execute.
            success_msg (str, optional): Message to print upon successful execution.
        """
        try:
            self.conn.sql(sql)
            if success_msg:
                print(success_msg)
        except Exception as e:
            print(f"Error: {e}")

    def query_sql(self, sql: str):
        """
        Executes a SELECT query and returns the result as a DataFrame.

        Args:
            sql (str): The SELECT SQL query.

        Returns:
            pd.DataFrame: Query result as a DataFrame.
        """
        try:
            result = self.conn.sql(sql).fetchdf()
            return result
        except Exception as e:
            print(f"Error during SELECT: {e}")
            return result

    def load_csv_to_table(self, table_name: str, file_path: str):
        """
        Loads data from a CSV file into a specified table in the database.

        Args:
            table_name (str): The target table name.
            file_path (str): The path to the CSV file.
        """
        try:
            self.conn.execute(f"""
                DELETE FROM {table_name};
                INSERT INTO {table_name}
                SELECT * FROM read_csv_auto('{file_path}')
            """)
            print(f"Data successfully loaded into {table_name}")
        except Exception as e:
            print(f"Error during loading data into {table_name}: {e}")

    def register_df(self, name: str, df):
        """
        Registers a pandas DataFrame with a given alias in DuckDB.

        Args:
            name (str): The alias name for the DataFrame.
            df (pandas.DataFrame): The DataFrame to register.
        """
        self.conn.register(name, df)

    def insert_from_df(self, table: str, df):
        """
        Inserts data from a pandas DataFrame into an existing table.

        Args:
            table (str): The table name to insert data into.
            df (pandas.DataFrame): The DataFrame containing data to insert.
        """
        self.register_df("temp_df", df)
        try:
            self.conn.sql(f"""
            DELETE FROM {table};
            INSERT INTO {table} BY NAME SELECT * FROM temp_df
            """)
            print(f"Inserted data into {table}")
        except Exception as e:
            print(f"Insert error into {table}: {e}")

    def close_db(self):
        self.conn.close()

