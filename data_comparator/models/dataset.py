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
from models.check import check_string_column, check_numeric_column, \
    check_boolean_column, check_temporal_column

logging.basicConfig(format='%(asctime)s - %(message)s')

ACCEPTED_INPUT_FORMATS = ['sas7bdat', 'csv', 'parquet', 'pyspark', 'pandas']


class Dataset:
    path = None
    input_format = ''
    size = ''
    dataframe = None
    columns = {}
    name = ''

    def __init__(self, data_src, name):
        self.name = name
        try:
            # probably a path string
            self.path = Path(data_src)
            self.input_format = self._get_input_format()
            self.size = self._get_data_size(data_src)
            self.dataframe = self.load_data_frompath()
        except TypeError:
            # probably an dataframe object
            self.input_format = str(data_src.__class__)
            if 'DataFrame' in self.input_format:
                self.dataframe = self.load_data_fromdf(
                    data_src
                )
                # count object types in size
                self.size = self.dataframe.memory_usage(deep=True).sum()

    def _get_input_format(self):
        suffix = self.path.suffix.replace('.', '')
        if suffix not in ACCEPTED_INPUT_FORMATS:
            raise ValueError('File type not supported')
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
            raise ValueError('File size of {} is too small'.format(size))
        if size > 800000000:
            raise ValueError('File size of {} is too large'.format(size))

        return size

    def load_data_frompath(self):
        print('Loading raw data into dataset object...')
        data = None
        if self.input_format == 'sas7bdat':
            data = pd.read_sas(str(self.path))
        elif self.input_format == 'csv':
            data = pd.read_csv(str(self.path))
        elif self.input_format == 'parquet':
            data = pd.read_parquet(str(self.path))
        else:
            raise ValueError('path type {} not recognized'.format(self.input_format))

        return data

    def load_data_fromdf(self, df):
        print('Loading raw data into dataset object...')
        data = None
        if 'pyspark' in self.input_format:
            data = df.toPandas()
        elif 'pandas' in self.input_format:
            data = df
        else:
            raise ValueError('object type not recognized')
        return data

    def prepare_columns(self):
        print("Preparing columns...")
        if  len(self.dataframe.columns) == 0:
            raise TypeError('No columns found for this dataframe')
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
    data = None
    name = ''
    count = 0
    invalid = 0
    missing = 0

    def __init__(self, raw_column):
        self.name = raw_column.name
        self.count = raw_column.count()
        self.missing = raw_column.isnull().sum()
        self.data = raw_column
  
    def __eq__(self, other_col):
        return other_col.__class__ == self.__class__


class StringColumn(Column):
    data_type = ''
    duplicates = 0
    unique = 0
    text_length_mean = 0
    text_length_std = 0
    top = ''

    def __init__(self, raw_column):
        Column.__init__(self, raw_column)
        self.data_type = self.__class__.__name__
        self.text_length_mean = raw_column.str.len().mean()
        self.text_length_std = raw_column.str.len().std()
        self.unique = raw_column.nunique()
        self.duplicates = self.count - self.unique

    def get_summary(self):
        summary = {
            'name': self.name, 'count': self.count, 'missing': self.missing, \
            'data_type': self.data_type, 'text_length_mean': self.text_length_mean, \
            'text_length_std': self.text_length_std, 'unique': self.unique, \
            'duplicates': self.duplicates
        }
        return summary

    def perform_column_check(self):
        return check_string_column(self)


class NumericColumn(Column):
    data_type = ''
    max = 0.0,
    min = 0.0,
    std = 0.0,
    mean = 0.0,
    zeros = 0

    def __init__(self, raw_column):
        Column.__init__(self, raw_column)
        self.data_type = self.__class__.__name__
        self.min = raw_column.min()
        self.max = raw_column.max()
        self.std = raw_column.std()
        self.mean = raw_column.mean()
        self.zeros = (raw_column == 0).sum()

    def get_summary(self):
        summary = {}
        summary['name'] = self.name
        summary['count'] = self.count
        summary['missing'] = self.missing
        summary['data_type'] = self.data_type
        summary['min'] = self.min
        summary['max'] = self.max
        summary['std'] = self.std
        summary['mean'] = self.mean
        summary['zeros'] = self.zeros
        return summary

    def perform_column_check(self):
        return check_numeric_column(self)


class TemporalColumn(Column):
    data_type = ''
    min = None
    max = None
    unique = 0

    def __init__(self, raw_column):
        Column.__init__(self, raw_column)

    def get_summary(self):
        summary = {}
        summary['data_type'] = self.__class__.__name__
        summary['min'] = None
        summary['max'] = None
        summary['unique'] = 0
        return summary

    def perform_column_check(self):
        return check_temporal_column(self)


class BooleanColumn(Column):
    data_type = ''
    top = ''
    
    def __init__(self, raw_column):
        Column.__init__(self, raw_column)
 
    def get_summary(self):
        summary = {}
        summary['data_type'] = self.__class__.__name__

    def perform_column_check(self):
        return check_boolean_column(self)
