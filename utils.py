from os import listdir, mkdir
from pandas import read_csv
from datetime import date, timedelta


def open_file(path, mode="r", encoding="utf-8", text=None):
    with open(path, mode=mode, encoding=encoding) as file:
        if mode == "r":
            return file.read()
        elif mode == "w":
            file.write(text)


def create_dir(path):
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


def load_manager(path: str, func):
    root, name = path.split('/')[1:]
    if name in listdir(root):
        df = read_csv(path)
    else:
        df = func()
        df.to_csv(path, index=False)
    return df