"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    data model for data file object

### DEVELOPER NOTES:
"""
import logging
import os
from pathlib import Path
import re

import pandas as pd

logging.basicConfig(format='%(asctime)s - %(message)s')

ACCEPTED_DATA_TYPES = ['sas7bdat', 'csv', 'parquet', 'pyspark', 'pandas']

class Dataset:
    path = None
    type = ''
    size = ''
    dataframe = None
    columns = {}

    def __init__(self, data_src):
        if not data_src:
            logging.error('Reference to data source is missing or invalid')
        try:
            # probably a path string
            self.path = Path(data_src)
            self.type = self._get_data_type()
            self.size = self._get_data_size(data_src)
            self.dataframe = self.load_data_frompath(
                self.path,
                self.type
            )
        except TypeError:
            # probably an dataframe object
            type = data_src.split('.')[0]
            if type in ['pyspark', 'pandas']:
                self.dataframe = self.load_data_fromdf(
                    self.path,
                    self.type
                )
                self.size = self.dataframe.size

    def _get_data_type(self):
        suffix = self.path.suffix.replace('.', '')
        if suffix not in ACCEPTED_DATA_TYPES:
            logging.error('file type not supported')
        return suffix

    def _get_data_size(self, data_src):
        size = 0
        if self.path.is_dir():
            for file in os.listdir(data_src):
                abs_path = os.path.join(data_src, file)
                size += os.path.getsize(abs_path)
        else:
            size = os.path.getsize(data_src)
        if size < 1:
            logging.error(size)
        return size

    def load_data_frompath(self, path, type):
        if type == 'sas7bdat':
            data = pd.read_sas(str(path))
        elif type == 'csv':
            data = pd.read_csv(str(path))
        elif type == 'parquet':
            data = pd.read_parquet(str(path))
        else:
            logging.error('path type not recognized')
        return data

    def load_data_fromdf(self, df, type):
        if type == 'pyspark':
            data = df.to_pandas(df)
        elif type == 'pandas':
            data = df
        else:
            logging.error('object type not recognized')
        return data

    def prepare_columns(self):
        for raw_col_name in self.dataframe.columns:
            raw_column = self.dataframe[raw_col_name]
            if re.search(r'(int)', str(raw_column.dtype)):
                self.columns[raw_col_name] = NumericColumn(raw_column)
            if re.search(r'(float)', str(raw_column.dtype)):
                self.columns[raw_col_name] = NumericColumn(raw_column)
            if re.search(r'(str)', str(raw_column.dtype)) or raw_column.dtype == 'O':
                self.columns[raw_col_name] = StringColumn(raw_column)
            if re.search(r'(time)', str(raw_column.dtype)):
                self.columns[raw_col_name] = TemporalColumn(raw_column)
            if re.search(r'(bool)', str(raw_column.dtype)):
                self.columns[raw_col_name] = BoolColumn(raw_column)


class Column:
    name = ''
    count = 0
    invalid = 0
    missing = 0
    type = ''
    data = None

    def __init__(self, raw_column):
        self.name = raw_column.name
        self.type = raw_column.dtype
        self.count = raw_column.count()
        self.missing = raw_column.isnull().sum()
        self.data = raw_column

class StringColumn(Column):
    duplicates = 0
    unique = 0
    text_length_mean = 0
    text_length_std = 0

    def __init__(self, raw_column):
        Column.__init__(self, raw_column)
        self.text_length_mean = raw_column.str.len().mean()
        self.text_length_std = raw_column.str.len().std()

class NumericColumn(Column):
    zeros = 0
    max = 0
    min = 0
    std = 0

    def __init__(self, raw_column):
        Column.__init__(self, raw_column)
        self.min = raw_column.min()
        self.max = raw_column.max()
        self.std = raw_column.std()

class TemporalColumn(Column):
    unique = 0
    max = None
    min = None

    def __init__(self, raw_column):
        Column.__init__(self, raw_column)

class BoolColumn(Column):

    def __init__(self, raw_column):
        Column.__init__(self, raw_column)
