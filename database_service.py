import time
import pandas as pd
import gspread, psycopg2

from google.oauth2 import service_account
from google.cloud import bigquery


class GoogleService:

    def __init__(self, token) -> None:
        SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/bigquery'
        ]
        cred = service_account.Credentials.from_service_account_info(token, scopes=SCOPES)
        self.__gs_client = gspread.authorize(credentials=cred)
        self.__bq_client = bigquery.Client(credentials=cred)

    def collect_spreadsheets(self, key, title):
        return self.__gs_client.open_by_key(key).worksheet(title).get_values()

    def collect_data(self, query):
        return self.__bq_client.query(query).to_dataframe()
    

class DWHService:

    def __init__(self, token) -> None:
        """DWH на PostgreSQL
        Params:
            :token: json - {
                "host": str,
                "database": str, 
                "user": str,
                "password": str
            }
        """
        self.__token = token

    def collect_data(self, query) -> pd.DataFrame:
        with psycopg2.connect(**self.__token) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
        
        return pd.DataFrame(cursor.fetchall(), columns=[col.name for col in cursor.description])
