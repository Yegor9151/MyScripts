from gspread import authorize
from pandas import DataFrame, read_sql_query
from pyodbc import connect

from google.oauth2.service_account import Credentials
from google.cloud.bigquery import Client


class QueryEditor:
    
    def __init__(self, temp):
        """
        Assemble query

        params:
            :temp: str = query template 
        """
        self.__temp = temp

    def replace(self, repl: dict):
        """
        Assemble query for DWH

        params:
            :repl: dict = {pattern_to_replace: new_value}
        return: str =  prepeared query
        """
        query = self.__temp
        for tag, val in repl.items():
            query = query.replace(tag, val)
        return query


class GoogleCollector:
    
    def __init__(self, google_token, table_info=None):
        """
        Collect data from google source
        
        params:
            :google_token: json = token for google sources
            :table_info: dict = {'title': of table, 'key': of table}
        """
        
        scopes = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://www.googleapis.com/auth/bigquery']
        
        # google authorization
        cred = Credentials.from_service_account_info(google_token, scopes=scopes)
        gs = authorize(cred)

        # Connect 
        # UTM-база
        if table_info:
            self.__ws = gs.open_by_key(table_info['key']).worksheet(table_info['title'])
        # BigQuery
        self.__big_client = Client(credentials=cred)
        
    def utm_label(self):
        """
        Collect utm marks from google table

        return: DataFrame with source name content
        """
        utm = self.__ws.get_all_values()

        utm = DataFrame(utm[1:]).loc[:, :2]
        utm.columns = ['source', 'source_name', 'content']
        utm.loc[utm['content'].str.len() < 2, 'content'] = None

        utm['source'] = utm['source'].str.replace('\t', '')
        utm['source'] = utm['source'].str.replace('"', '')
        utm['source'] = utm['source'].str.replace(' ', '')
        utm['source'] = utm['source'].str.split('/', expand=True)[0]
        return utm
    
    def collect(self, query):
        """
        Collect data from BigQuery

        params:
            :query: query for data search
        return: DataFrame with search result
        """
        return self.__big_client.query(query).result().to_dataframe()


class DWHCollector:
    
    def __init__(self, server_ip, database):
        """
        Collect data from DWH
        
        params:
            :server_ip: str
            :database: str
        """
        self.__connection = connect(
            "Driver={SQL Server Native Client 11.0};"
            f"Server={server_ip};"
            f"Database={database};"
            "Trusted_Connection=yes;"
            "autocommit=True;"
        )
    
    def collect(self, query: str):
        """
        Collect data

        params:
            :query: str = query for collect data
        return: DataFrame with result
        """
        df = read_sql_query(query, self.__connection)
        return df