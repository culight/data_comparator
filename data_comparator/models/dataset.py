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
    data_type = ''
    size = ''
    dataframe = None
    columns = {}
    name = ''

    def __init__(self, data_src, name):
        self.name = name
        try:
            # probably a path string
            self.path = Path(data_src)
            self.data_type = self._get_data_type()
            self.size = self._get_data_size(data_src)
            self.dataframe = self.load_data_frompath()
        except TypeError:
            # probably an dataframe object
            self.data_type = str(data_src.__class__)
            if 'DataFrame' in self.data_type:
                self.dataframe = self.load_data_fromdf(
                    data_src
                )
                # count object types in size
                self.size = self.dataframe.memory_usage(deep=True).sum()

    def _get_data_type(self):
        suffix = self.path.suffix.replace('.', '')
        if suffix not in ACCEPTED_DATA_TYPES:
            logging.error('File type not supported')
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

    def load_data_frompath(self):
        data = None
        if self.data_type == 'sas7bdat':
            data = pd.read_sas(str(self.path))
        elif self.data_type == 'csv':
            data = pd.read_csv(str(self.path))
        elif self.data_type == 'parquet':
            data = pd.read_parquet(str(self.path))
        else:
            logging.error('path type not recognized')
        return data

    def load_data_fromdf(self, df):
        data = None
        if 'pyspark' in self.data_type:
            data = df.toPandas()
        elif 'pandas' in self.data_type:
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
                self.columns[raw_col_name] = BooleanColumn(raw_column)


class Column:
    name = ''
    count = 0
    invalid = 0
    missing = 0
    data = None
    this_class = None

    def __init__(self, raw_column):
        self.name = raw_column.name
        self.count = raw_column.count()
        self.missing = raw_column.isnull().sum()
        self.data = raw_column
        self.this_class = self.__class__

    def __eq__(self, other_col):
        return other_col.__class__ == self.__class__


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

class BooleanColumn(Column):

    def __init__(self, raw_column):
        Column.__init__(self, raw_column)