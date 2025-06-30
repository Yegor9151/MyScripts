from airflow.models import Variable
from datetime import datetime
import clickhouse_connect, pymongo, yadisk, gspread, time
from google.oauth2 import service_account
from googleapiclient.discovery import build

class Creds:
    
    def google():
        return Variable.get("GOOGLE_DRIVE_TOKEN", deserialize_json=True)
    
    def yadisk(): 
        return Variable.get("YADISK_TOKEN")
    
    def clickhouse(send_receive_timeout=1800):
        return {
            "host":Variable.get('CLICKHOUSE_HOST'),
            "port":Variable.get('CLICKHOUSE_PORT'),
            "user":Variable.get('CLICKHOUSE_USER'),
            "password":Variable.get('CLICKHOUSE_PASSWORD'),
            "send_receive_timeout": send_receive_timeout
        }
    
    def mongodb(cluster="pmnt"):
        MONGODB_HOST = {
            "pmnt": "MONGODB_PAYMENT_HOST",
            "powerbank": "MONGODB_POWERBANKS_HOST"
        }

        return{
            "host": Variable.get(MONGODB_HOST[cluster]),
            "port": int(Variable.get('MONGODB_PORT')),
            "username": Variable.get('MONGODB_USER'),
            "password": Variable.get('MONGODB_PASSWORD'),
            "authsource": "admin",
            "readpreference": "secondary",
            "directconnection": True
        }
    
    def telegram():
        return Variable.get("TELEGRAM_BOT_TOKEN")


class Clickhouse:

    def __init__(self, creds):
        self.client = clickhouse_connect.get_client(**creds)

    def query_df(self, query, retries=5, timeout=30):

        for i in range(retries):
            try:
                return self.client.query_df(query)
            except TimeoutError as err:
                print("Ошибка:", err)

            time.sleep(timeout)
            print(f"Попытка", i+2)

        assert False, "Не удалось собрать данные - возможно запрос слишком большой"


class Client:

    def googlesheets():
        return gspread.service_account_from_dict(Creds.google())

    def yadisk():
        return yadisk.YaDisk(token=Creds.yadisk())

    def clickhouse(send_receive_timeout=1800):
        return Clickhouse(Creds.clickhouse(send_receive_timeout))
    
    def mongodb(cluster="pmnt"):
        return pymongo.MongoClient(**Creds.mongodb(cluster))
    
    def google_drive():
        """Создает сервис для работы с Google Drive API"""
        credentials = service_account.Credentials.from_service_account_info(
            Creds.google(),
            scopes=['https://www.googleapis.com/auth/drive']
        )
        return build('drive', 'v3', credentials=credentials)


class DataChecker:

    def __init__(self):
        """Класс для проверки наличия данных.
        Может проверять наличие данных в источнике несколько раз если задать параметры в соответствующем методе

        Пример испольхования:

        from src import specific

        data_checker = DataChecker()
        data_checker.clickhouse(query)
        """

    def clickhouse(self, query, retries=5, timeout=600, send_receive_timeout=1800, success=None, client=None):
        """query : str - на вход ожидает SQL запрос, который возвращает: 
        - 1 или True - данные есть в наличие
        - 0 или False - данных нет в наличие
        
        retries : int - число попыток
        timeout : int - перерыв между попытками в секундах
        success : any - сообщение при успешной проверке, можно оставить True, что бы использовать в последующей логике
        send_receive_timeout : int - сколько секунд может выполняться запрос
        client : - клиен clickhouse, если не присваивать, создается автоматически
        """

        client = client if client else Client.clickhouse(send_receive_timeout)

        for i in range(retries):

            result = client.query_df(query).iloc[0, 0]
            
            print(f"Попытка {i+1} из {retries}:", datetime.now(), "\nРезультат: {result} - {'данные есть в полном объёме' if result == 1 else 'данные не готовы'}")

            if result:
                return success if success else result

            time.sleep(timeout)

        return "Попытки закончились, проверьте подключение к серверу"
