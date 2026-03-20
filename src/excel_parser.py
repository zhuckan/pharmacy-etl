import pandas as pd

class ExcelParser:
    def __init__(self, skiprows=None, columns=None):
        self.__skiprows = skiprows
        self.__columns = columns

    def parse(self, path):
        df = pd.read_excel(path, skiprows=self.__skiprows, header=None, usecols=range(1, 7))
        return df

    def parse_data(self, df):
        if df.shape[0] == 0:
            raise ValueError("файл не содержит данных")
        df.columns = self.__columns
        df['pharmacyNumber'] = df['pharmacyNumber'].apply(self.__clean_pharmacy_number)
        return df

    def __clean_pharmacy_number(self, value):
        if pd.isna(value):
            return None
        if isinstance(value, (int, float)):
            return value
        return None
