import json
import platform
from pathlib import Path
from typing import Dict, Union
import time

import cx_Oracle
import sqlalchemy
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import urllib


def init_oracle_client(lib_dir=None):
    # The lib_dir parameter is important only for Windows platform

    if platform.system() == 'Windows':
        oracle_dir = lib_dir
    else:
        oracle_dir = None
    try:

        cx_Oracle.init_oracle_client(oracle_dir)
        print("Oracle Client initialized successfully.")
    except cx_Oracle.ProgrammingError as e:
        if "already been initialized" in str(e):
            print("Oracle Client library has already been initialized.")
        else:
            raise Exception


class DatabaseQuery:
    def __init__(self,
                 source: str = 'BI',
                 passcode_path: Union[str, Path] = 'passcode.json',
                 table_info_path: Union[str, Path] = 'stored_info.json',
                 save_data=False,
                 output_path: Union[str, Path] = './output_data',
                 output_format: str = 'csv',
                 oracle_client_path=None):
        """
        Initialize the DatabaseQuery object with the source database configuration.
        
        Args:
            source (str): The source database identifier in the configuration.
            passcode_path (Union[str, Path]): Path to the passcode JSON file.
            table_info_path (Union[str, Path]): Path to the table info JSON file.
            output_path (Union[str, Path]): Directory to save the output DataFrame.
            output_format (str): Format to save the output DataFrame ('csv' or 'parquet').
        """
        self.source = source
        self.passcode_path = Path(passcode_path)
        self.table_info_path = Path(table_info_path)
        self.output_path = Path(output_path)
        self.output_format = output_format.lower()
        self.oracle_client_path = oracle_client_path
        # Flag to choose whether or not to save the data
        self.save_data = save_data

        # Load configuration files
        self.data_dict = self._load_json(self.passcode_path)
        if self.source == "BI":
            self.table_info_dict = self._load_json(self.table_info_path)

        self.conn_str = self._build_connection_string()
        self.engine = self._create_engine()

    def _load_json(self, file_path: Path) -> Dict:
        """
        Load a JSON file and return its content.
        
        Args:
            file_path (Path): Path to the JSON file.
        
        Returns:
            Dict: Content of the JSON file.
        """
        if not file_path.exists():
            print(f"Error: JSON file not found at {file_path}")
            raise FileNotFoundError(f"JSON file not found at {file_path}")
        with open(file_path, 'r') as file:
            print(f"Loading JSON file from {file_path}")
            return json.load(file)

    def _build_connection_string(self) -> str:
        """
        Construct the database connection string using the source information.
        
        Returns:
            str: The database connection string.
        """
        db_names = self.data_dict.get('DB_NAMES')
        if not db_names or self.source not in db_names:
            print("Error: Source database configuration not found in JSON file.")
            raise ValueError("Source database configuration not found in JSON file.")

        passcodes = db_names[self.source]
        if self.source == "BI":
            server, db, uid, pwd, driver = (
                passcodes['Server'],
                passcodes['Database'],
                passcodes['UID'],
                passcodes['PWD'],
                passcodes['driver'],
            )

            conn_str = f'DRIVER={driver};Server={server};Database={db};UID={uid};PWD={pwd};charset=utf8;Connection Timeout=30; '
            # format the connt_str
            conn_str = f'mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}'


        elif self.source == 'ORACLE_LIVE':
            init_oracle_client(self.oracle_client_path)
            # Adjusted SQLAlchemy connection string using Easy Connect syntax.
            password = urllib.parse.quote_plus(passcodes['psw'])  # URL-encode the password
            conn_str = f"oracle+cx_oracle://{passcodes['user']}:{password}@{passcodes['host']}:{passcodes['port']}/{passcodes['service']}"

        print(conn_str)
        return conn_str

    def _create_engine(self) -> Engine:
        """
        Create a SQLAlchemy engine for the database connection.
        
        Returns:
            Engine: SQLAlchemy engine for database operations.
        """

        print("Creating database engine.")
        if self.source == 'BI':
            return sqlalchemy.create_engine(self.conn_str, fast_executemany=True)
        else:
            return sqlalchemy.create_engine(self.conn_str)

    def fetch_data(self, first_date: str = None, last_date: str = None, specialty: str = None,
               insurance: str = None, orcale_query: str = None, max_retries: int = 3, 
               retry_delay: int = 5) -> pd.DataFrame:
        """
        Fetch data from the database within the specified date range with retry mechanism.
        
        Args:
            first_date (str): The start date in 'YYYY-MM-DD' format.
            last_date (str): The end date in 'YYYY-MM-DD' format.
            specialty (str): The specialty filter.
            insurance (str): The insurance filter.
            orcale_query (str): The Oracle query to execute.
            max_retries (int): Maximum number of retry attempts. Default is 3.
            retry_delay (int): Delay between retries in seconds. Default is 5.
            
        Returns:
            pd.DataFrame: The result of the SQL query.
        """
        if self.source == "BI":
            columns = self._get_query_columns()
            query = self._build_query(columns, first_date, last_date, specialty, insurance)
        elif self.source == "ORACLE_LIVE":
            columns = None  # no need for the columns here
            query = self._build_query(columns, first_date, last_date, specialty, insurance, orcale_query=orcale_query)
        else:
            raise ValueError("Undefined source")

        # Initialize retry counter
        retry_count = 0
        last_exception = None
        
        # Try to fetch data with retries
        while retry_count <= max_retries:
            # initially define the connection as None
            connection = None
            try:
                connection = self.engine.connect()
                print(f"Executing SQL query (attempt {retry_count + 1}/{max_retries + 1}).")
                df = pd.read_sql(query, connection)
                if self.save_data:
                    self._save_dataframe(df)
                return df  # Return immediately on success
                
            # add handling exception
            except SQLAlchemyError as e:
                last_exception = e
                print(f"[ERROR] SQL execution failed (attempt {retry_count + 1}/{max_retries + 1}): {e}")
                # If this connection is in a broken transaction, try to rollback
                try:
                    if connection is not None and connection.in_transaction():
                        connection.rollback()
                except Exception as rollback_error:
                    print(f"[WARNING] Rollback failed: {rollback_error}")
                
                # Increment retry counter
                retry_count += 1
                
                # If we haven't reached max retries, wait and try again
                if retry_count <= max_retries:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                
            finally:
                if connection is not None:
                    connection.close()
        
        # If we've exhausted all retries, raise the last exception
        print(f"[ERROR] Failed to fetch data after {max_retries + 1} attempts.")
        raise last_exception


    def _get_query_columns(self) -> Dict[str, str]:
        """
        Build a dictionary of SQL column strings by table alias.
        
        Returns:
            Dict[str, str]: A dictionary with SQL column strings.
        """
        return {
            'visit_columns': ', '.join([f'V.[{col}]' for col in self.table_info_dict['claim_visit_columns']]),
            'service_columns': ', '.join([f'S.[{col}]' for col in self.table_info_dict['claim_service_columns']]),
            'diagnose_columns': ', '.join([f'E.[{col}]' for col in self.table_info_dict["diagnosis_columns"]]),
            'complaint_columns': ', '.join([f'C.[{col}]' for col in self.table_info_dict["chief-complaint_columns"]]),
            'purchaser_columns': ', '.join([f'P.[{col}]' for col in self.table_info_dict["purchaser_contract_columns"]])
        }

    def _build_query(self, columns: Dict[str, str] = None, start_date: str = None, end_date: str = None,
                     specialty: str = None,
                     insurance: str = None, orcale_query: str = None) -> str:
        """
        Construct the SQL query with dynamic columns and date filters.
        
        Args:
            columns (Dict[str, str]): Dictionary with SQL column strings.
            first_date (str): Start date for filtering.
            last_date (str): End date for filtering.
        
        Returns:
            str: The constructed SQL query.
        """
        if self.source == "BI":
            query = f"""
                SELECT 
                    {columns['visit_columns']}, 
                    {columns['service_columns']}, 
                    {columns['diagnose_columns']},
                    {columns['complaint_columns']},
                    {columns['purchaser_columns']}
                FROM Claim_Visit V
                LEFT JOIN Claim_Service S ON V.VISIT_ID = S.VISIT_ID
                LEFT JOIN [dbo].[Last_Diagnosis] E ON E.VISIT_NO = V.VISIT_NO
                LEFT JOIN [dbo].[Chief_Complaint] C 
                    ON C.Visit_NO = V.VISIT_NO AND ISNULL(C.Updated_Date, C.Created_Date) = V.VISIT_DATE
                LEFT JOIN [dbo].[purchaser_Contract] P
                    ON P.PURCHASER_CODE = V.PURCHASER_CODE AND P.CONTRACT_NO = V.CONTRACT_NO
                WHERE 
                    (V.[CREATION_DATE] >= '{start_date}' AND V.[CREATION_DATE] <= '{end_date}')
                    OR 
                    (V.[AMEND_LAST_DATE] >= '{start_date}' AND V.[AMEND_LAST_DATE] <= '{end_date}')
            """

            if specialty:
                query += f" AND V.PROVIDER_DEPARTMENT = '{specialty}' "
            if insurance:
                query += f" AND P.PUR_NAME = '{insurance}' "

        elif self.source == "ORACLE_LIVE":
            query = orcale_query
        return query

    def _save_dataframe(self, df: pd.DataFrame):
        """
        Save the DataFrame to the configured output path in the specified format.
        
        Args:
            df (pd.DataFrame): The DataFrame to save.
        """
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        if self.output_format == 'csv':
            file_path = self.output_path.with_suffix('.csv')
            df.to_csv(file_path, index=False)
        elif self.output_format == 'parquet':
            file_path = self.output_path.with_suffix('.parquet')
            df.to_parquet(file_path)
        else:
            print(f"Error: Unsupported output format: {self.output_format}")
            raise ValueError(f"Unsupported output format: {self.output_format}")

        print(f"DataFrame saved to {file_path}")


# # Usage example:
# db_query = DatabaseQuery(
#     source='BI',
#     passcode_path=r'E:\AI_Projects\Claims_Rejection_2024\Claims_Rejection\src\data_backup\passcode.json',
#     table_info_path=r'E:\AI_Projects\Claims_Rejection_2024\Claims_Rejection\src\data_backup\stored_info.json',
#     output_path=r'data/train/df_train.parquet',
#     output_format='parquet'
# )
# result_df = db_query.fetch_data('2024-01-01', '2024-01-02')
# print(result_df.head())


if __name__ == '__main__':
    db_query = DatabaseQuery(
        source='ORACLE_LIVE',
        passcode_path=r'/home/ai/Workspace/XplainClaims/passcode.json',
        table_info_path=r'stored_info.json',
        output_path=r'data/sample/df_sample.parquet',
        output_format='parquet',
        save_data=True  # should be False if you don't want to save the data locally
    )
    result_df = db_query.fetch_data()
    print(result_df.head())