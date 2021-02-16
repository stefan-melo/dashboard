from icecream import ic
from configparser import ConfigParser
from abc import ABC, abstractclassmethod
from enum import Enum
from os import path, listdir
import pandas as pd


config = ConfigParser()
config.read('config.ini')


class SourceType(Enum):
    CSV = 1
    EXCEL = 2
    JSON = 3

class ConnType(Enum):
    FILE = 1
    DIRECTORY = 2

file_extension = {
    SourceType.CSV: '.csv',
    SourceType.EXCEL: '.xlsx',
    SourceType.JSON: '.json'
}

def join_path(dir, files):
        """
        Create full path strings: "dir" + [files].\n\n
        dir:    static path string\n
        files:  list of file name strings
        """
        complete = []
        for f in files:
            complete.append(path.join(dir, f))
        return complete


class Source(ABC):
    """
    Source interface\n
    Represents a data source\n
    Source "type" and "conn" string is needed.
    """
    def __init__(self, conn):
        self.conn_string = conn
        self.conn_type = None
 

        if path.isfile(self.conn_string):
            self.conn_type = ConnType.FILE
        if path.isdir(self.conn_string):
            self.conn_type = ConnType.DIRECTORY
            files = [f for f in listdir(self.conn_string) if f.endswith(file_extension[self.type])] 
            self.conn_string = join_path(self.conn_string, files)


    @abstractclassmethod
    def validate(self):
        return self.conn_string


class CSV_Source(Source):

    type = SourceType.CSV

    def validate(self):
        return self.conn_string


class Excel_Source(Source):

    type = SourceType.EXCEL

    def validate(self):
        return self.conn_string


class Data(ABC):
    """
    Data interface\n
    Represents general data
    """
    def __init__(self, source):
        self.source = source

    @abstractclassmethod
    def read(self):
        """
        Reads data
        """
        pass


class DF_Data(Data):
    """
    DataFrame representation
    """
    def __init__(self, source):
        super().__init__(source)

    def read(self):
        """
        Reads data and returns DataFrame
        """
        switch = {
            SourceType.CSV: pd.read_csv,
            SourceType.JSON: pd.read_json,
            SourceType.EXCEL: pd.read_excel
        }
        
        if self.source.conn_type == ConnType.DIRECTORY:
            df = pd.concat(map(switch[self.source.type], self.source.validate()))
        else:
            df = switch[self.source.type](self.source.validate())

        return df
        

ic.configureOutput(includeContext=False)
ic('Starting...')

con1 = config['DATA SOURCE']['DataSource1']
con2 = config['DATA SOURCE']['DataSource2']

s1 = CSV_Source(con1)
s2 = Excel_Source(con2)

ic(s1.conn_type)
ic(s2.conn_type)
ic(s1.type)
ic(s2.type)

ic(s1)

data1 = DF_Data(s1)
data2 = DF_Data(s2)

df1 = data1.read()
df2 = data2.read()

ic(df1.head())
ic(df2.head())
