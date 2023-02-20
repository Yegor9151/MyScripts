from os import listdir, mkdir
from pandas import read_csv
from datetime import date, timedelta
from requests import post


class TempEditor:
    
    def __init__(self, temp):
        """
        Assemble query

        params:
            :temp: str = query template 
        """
        self.temp = temp
        
    def get_temp(self):
        return self.temp

    def replace(self, repl: dict):
        """
        Assemble query for DWH

        params:
            :repl: dict = {pattern_to_replace: new_value}
        return: str =  prepeared query
        """
        query = self.temp
        for tag, val in repl.items():
            query = query.replace(tag, val)
        return query


def open_file(path, mode="r", encoding="utf-8", text=None):
    with open(path, mode=mode, encoding=encoding) as file:
        if mode == "r":
            return file.read()
        elif mode == "w":
            file.write(text)


def create_dir(path: str) -> bool:
    """Create dir
    
    params: 
        path: str - path to dir
    return: bool"""
    
    try:
        mkdir(path)
        return True
    except FileExistsError as e:
        print(e)
        return False


def last_month():
    today = date.today()
    date2 = today - timedelta(days=today.day)
    date1 = date2 - timedelta(days=date2.day - 1)
    return date1, date2


def half_month():
    today = date.today()
    date1 = date(year=today.year, month=today.month, day=1)
    date2 = date(year=today.year, month=today.month, day=15)
    return date1, date2


def load_manager(path: str=None, func=None, low_memory=True, memory_map=False):
    
    if not path:
        return func()
    
    parts = path.split('/')
    if parts[-1] in listdir("/".join(parts[:-1])):
        return read_csv(path, low_memory=low_memory, memory_map=memory_map)
    
    df = func()
    df.to_csv(path, index=False)
    return df


class TeleBot:

    def __init__(self, token, chat_id):

        self.__TOKEN = token
        self.__CHAT_ID = chat_id

    def send_documents(self, files_dir):
        """push data in telegram iteratively
        
        Params:
            :files_dir - direction with files to upload in telegram chat
        Return: True if all is done"""

        URL = f'https://api.telegram.org/bot{self.__TOKEN}/sendDocument?chat_id={self.__CHAT_ID}'

        for file in listdir(files_dir):
            if '.zip' in file:
                path = f'{files_dir}/{file}'
                file = {'document': open(path, 'rb')}
                post(URL, files=file, verify=False)

        return URL

    def send_message(self, text):

        URL = f'https://api.telegram.org/bot{self.__TOKEN}/sendMessage?chat_id={self.__CHAT_ID}&text={text}'

        post(URL, verify=False)

        return URL