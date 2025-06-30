import re
import numpy as np
import pandas as pd
from os import listdir, mkdir
import datetime
# from datetime import datetime, date, timedelta
from requests import post

import smtplib, json
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText


def open_file(path, mode="r", encoding=None, text=None):
    with open(path, mode=mode, encoding=encoding) as file:
        if mode == "r" or mode == "rb":
            return file.read()
        elif mode == "w":
            file.write(text)


def read_json(path, encoding=None):
    return json.loads(open_file(path, mode="r", encoding=encoding))


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


def create_path(path):

    files = path.split('/') # разбиваем путь на файлы
    if '.' in files[-1]: # если последний файл имеет расширение
        files = files[:-1] # тогда исключаем этот файл из пути

    path = files[0] # берем 1й файл
    for d in files[1:]: # перебираем последующие в пути файлы
        path += f"/{d}" # к предыдущему файлу в пути прибавляем следующий
        create_dir(path) # достраиваем путь

    return path


def this_week(dt=None, to_current=True):

    de = dt if dt else datetime.date.today()

    ds = de - datetime.timedelta(days=de.weekday())
    
    if not to_current:
        de = ds + datetime.timedelta(days=6)

    return ds, de


def this_month(dt=None, to_current=True):
    de = dt if dt else datetime.date.today()
    
    if not to_current:
        
        next_month = de.month + 1
        if next_month > 12:
            de = de.replace(year=de.year+1, month=1, day=1) - datetime.timedelta(days=1)
        else:
            de = de.replace(month=next_month, day=1) - datetime.timedelta(days=1)

    ds = de.replace(day=1)
    return ds, de


def prev_month(dt=None):
    de = dt if dt else datetime.date.today()
    de = de.replace(day=1) - datetime.timedelta(days=1)
    ds = de - datetime.timedelta(days=de.day - 1)
    return ds, de


def half_month():
    today = datetime.date.today()
    date1 = datetime.date(year=today.year, month=today.month, day=1)
    date2 = datetime.date(year=today.year, month=today.month, day=15)
    return date1, date2


def load_manager(path: str=None, func=None, low_memory:bool=True, memory_map:bool=False, parse_dates:list=False):
    """
    Need to ones preprocess data and save ones, then automaticaly upload precessed data
    params:
        path: str - path to save or upload data
        func: - lambda function to processing data

        https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
        low_memory: bool - internally process the file in chunks, if memory not enough to upload
        memory_map: bool - can help to upload massive data
        parse_datas: list - parse input columns to datetime type
    """

    # 1. if path not input, then use lambda function ewerytime
    if not path:
        return func()
    
    # 2. if path input, and file exist then upload file
    parts = path.split('/')
    if parts[-1] in listdir("/".join(parts[:-1])):
        return pd.read_csv(path, low_memory=low_memory, memory_map=memory_map, parse_dates=parse_dates)
    
    # 3. if path input, but file not exist, then run lambda function, and save .csv result
    data = func()
    try:
        if data.shape[0]:
            data.to_csv(path, index=False)
    except TypeError as err:
        open_file(path, 'w', text=data)
        print(err)

    return data


class TelegramBot:

    def __init__(self, token):

        self.__TOKEN = token

    def send_documents(self, chat_id, files_dir):
        """push data in telegram iteratively
        
        Params:
            :files_dir - direction with files to upload in telegram chat
        Return: True if all is done"""

        URL = f'https://api.telegram.org/bot{self.__TOKEN}/sendDocument?chat_id={chat_id}'

        for file in listdir(files_dir):
            if '.zip' in file:
                path = f'{files_dir}/{file}'
                file = {'document': open(path, 'rb')}
                post(URL, files=file, verify=False)

        return URL

    def send_message(self, chat_id, text):

        URL = f'https://api.telegram.org/bot{self.__TOKEN}/sendMessage?chat_id={chat_id}&text={text}'
        payload = {"parse_mode": "HTML"}

        post(URL, data=payload, verify=False)

        return URL


def parse_url(url):

    root, params = url.split("?")
    params = dict([p.split("=", 1) for p in params.split("&")])
    
    return root, params


def reduce_memory(df: pd.DataFrame):
    
    print("memory before:", round(df.memory_usage().sum() / (2**20), 4), "MB")

    for col in df.columns:
        
        if "int" in str(df[col].dtype):
            
            if not (df[col] != df[col].astype(f"int8")).sum():
                df[col] = df[col].astype(f"int8")
            elif not (df[col] != df[col].astype(f"int16")).sum():
                df[col] = df[col].astype(f"int16")
            elif not (df[col] != df[col].astype(f"int32")).sum():
                df[col] = df[col].astype(f"int32")
            elif not (df[col] != df[col].astype(f"int64")).sum():
                df[col] = df[col].astype(f"int64")
        
        elif "float" in str(df[col].dtype):
            
            if not (df[col] != df[col].astype(f"float16")).sum():
                df[col] = df[col].astype(f"int16")
            elif not (df[col] != df[col].astype(f"float32")).sum():
                df[col] = df[col].astype(f"int32")
            elif not (df[col] != df[col].astype(f"float64")).sum():
                df[col] = df[col].astype(f"int64")
                
    print("memory after:", round(df.memory_usage().sum() / (2**20), 1), "MB")
    
    return df


class Temp:
    
    def __init__(self, temp):
        """
        Assemble query

        params:
            :temp: str = query template 
        """
        self.temp = temp

    def __repr__(self):
        return self.temp

    def replace(self, repl: dict):
        """
        Assemble query for DWH

        params:
            :repl: dict = {pattern_to_replace: new_value}
        return: str
        """
        temp = self.temp
        for tag, val in repl.items():
            temp = temp.replace(tag, str(val))
        return temp
    
    def add(self, lines: list[str], sep="\n"):
        """
        Add lines in end of text
        
        params:
           :lines: list = [string1, string2, ...]
        return: str
        """
        for l in lines:
            if l not in self.temp:
                self.temp += sep + l
        return self.temp


class Email:
    
    def __init__(self, host, port, user, password):
        
        self.__HOST = host
        self.__PORT = port
        self.__USER = user
        self.__PASSWORD = password
        
        self.__msg = MIMEMultipart()
        
    def __repr__(self):
        return f"HOST={self.__HOST}, PORT={self.__PORT}, USER={self.__USER}"
        
    def add_document(self, path):
        
        part = open_file(path, "rb")
        part = MIMEApplication(part)
        part['Content-Disposition'] = f"attachment; filename=\"{path.split('/')[-1]}\""

        self.__msg.attach(part)
        
        return self.__msg
        
    def add_message(self, text):
        
        self.__msg.attach(MIMEText(text))
        
        return self.__msg
        
    def push(self, from_address:str, to_address:list, title=None):
        
        with smtplib.SMTP(self.__HOST, self.__PORT) as server:
            server.starttls()
            server.login(self.__USER, self.__PASSWORD)
            
            self.__msg["From"] = from_address
            self.__msg["To"] = ", ".join(to_address)
            self.__msg["Subject"] = title

            server.send_message(self.__msg)

            del self.__msg
            
            return True


class DFPreprocessor:

    def __init__(self, df:pd.DataFrame):
        self.__df = df.copy()

    def __repr__(self):
        return str(self.__df)

    def get_df(self):
        return self.__df

    def drop_cols(self, columns:list):
        
        columns = self.__df.columns.isin(columns)
        columns = self.__df.columns[columns]
        self.__df = self.__df.drop(columns, axis=1)
        
        return self.__df

    def execute_str(self, columns):

        for col in columns:

            # проверки
            if type(self.__df.loc[0, col]) != str: # В этой колонке строки?
                break

            self.__df[col] = self.__df[col].apply(lambda x: re.sub(r"Decimal\d+[^\d]+|'\)", '', x))
            self.__df[col] = self.__df[col].apply(lambda x: eval(x, {"datetime": datetime}))

        return self.__df
            

    def explode(self, columns):

        for col in columns:
            self.__df = self.__df.explode(col).reset_index(drop=True)

        return self.__df

    def json_normalize(self, columns):

        for col in columns:
            self.__df = pd.concat([
                self.__df, pd.json_normalize(self.__df[col])
            ], axis=1)

        return self.__df

    def list_to_onehot(self, columns:list):

        for col in columns:
            for values in np.unique(self.__df[col]):
                rows = self.__df[col].apply(lambda x: x == values)
                self.__df.loc[rows, values] = 1
                self.__df[values] = self.__df[values].fillna(0)

        return self.__df
