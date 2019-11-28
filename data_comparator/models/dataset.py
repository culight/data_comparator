"""
### CODE OWNERS: Demerrick Moton
### OBJECTIVE:
    data model for data file object
### DEVELOPER NOTES:
"""
import logging
import os
from datetime import datetime
from pathlib import Path
import re
import pandas as pd
from models.check import check_string_column, check_numeric_column, \
    check_boolean_column, check_temporal_column

logging.basicConfig(format='%(asctime)s - %(message)s')

ACCEPTED_INPUT_FORMATS = ['sas7bdat', 'csv', 'parquet', 'pyspark', 'pandas', 'json']

class Dataset(object):
    def __init__(self, data_src: object, name: str, **load_params):
        self.path = None
        self.input_format = ''
        self.size = ''
        self.dataframe = None
        self.columns = {}
        self.name = name
        self.load_time = 0.0
        try:
            # probably a path string
            self.path = Path(data_src)
            self.input_format = self._get_input_format()
            self.size = self._get_data_size(data_src)
            self.dataframe = self.load_data_frompath(**load_params)
        except TypeError:
            # probably an dataframe object
            self.input_format = str(data_src.__class__)
            if 'DataFrame' in self.input_format:
                self.dataframe = self.load_data_fromdf(
                    data_src
                )
                # count object types in size
                self.size = self.dataframe.memory_usage(deep=True).sum()

        # try to categorize the columns
        self._prepare_columns()

    def _get_input_format(self) -> str:
        suffix = self.path.suffix.replace('.', '')
        if suffix not in ACCEPTED_INPUT_FORMATS:
            raise ValueError('File type not supported')
        return suffix
    
    def _format_size(self, size):
        'Return the given bytes as a human friendly KB, MB, GB, or TB string'
        B = float(size)
        KB = float(1024)
        MB = float(KB ** 2) # 1,048,576
        GB = float(KB ** 3) # 1,073,741,824
        TB = float(KB ** 4) # 1,099,511,627,776

        if size < KB:
            return '{0} {1}'.format(B,'Bytes' if 0 == size > 1 else 'Byte')
        elif KB <= size < MB:
            return '{0:.2f} KB'.format(size/KB)
        elif MB <= size < GB:
            return '{0:.2f} MB'.format(size/MB)
        elif GB <= size < TB:
            return '{0:.2f} GB'.format(size/GB)
        elif TB <= size:
            return '{0:.2f} TB'.format(size/TB)

        return size

    def _get_data_size(self, data_src: object) -> int:
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
        
        formatted_size = self._format_size(size)
        return formatted_size

    def load_data_frompath(self, **load_params) -> pd.DataFrame:
        print('\nLoading raw data into dataset object...')
        data = None
        start_time = datetime.now()
        if self.input_format == 'sas7bdat':
            data = pd.read_sas(str(self.path), **load_params)
        elif self.input_format == 'csv':
            data = pd.read_csv(str(self.path), **load_params)
        elif self.input_format == 'parquet':
            data = pd.read_parquet(str(self.path), **load_params)
        elif self.input_format == 'json':
            data = pd.read_json(str(self.path), **load_params)
        else:
            raise ValueError('Path type {} not recognized'.format(self.input_format))
        end_time = datetime.now()
        self.load_time = str(end_time - start_time)
        return data

    def load_data_fromdf(self, df) -> pd.DataFrame:
        print('\nLoading raw data into dataset object...')
        data = None
        start_time = datetime.now()
        if 'pyspark' in self.input_format:
            data = df.toPandas()
        elif 'pandas' in self.input_format:
            data = df
        else:
            raise ValueError('object type not recognized')
        end_time = datetime.now()
        self.load_time = str(end_time - start_time)
        return data

    def _prepare_columns(self):
        print("\nPreparing columns...")
        if  len(self.dataframe.columns) == 0:
            raise TypeError('No columns found for this dataframe')
        for raw_col_name in self.dataframe.columns:
            raw_column = self.dataframe[raw_col_name]
            if re.search(r'(int)', str(raw_column.dtype)):
                self.columns[raw_col_name] = NumericColumn(raw_column, self.name)
            if re.search(r'(float)', str(raw_column.dtype)):
                self.columns[raw_col_name] = NumericColumn(raw_column, self.name)
            if re.search(r'(str)', str(raw_column.dtype)) or str(raw_column.dtype) == 'object':
                self.columns[raw_col_name] = StringColumn(raw_column, self.name)
            if re.search(r'(time)', str(raw_column.dtype)):
                self.columns[raw_col_name] = TemporalColumn(raw_column, self.name)
            if re.search(r'(bool)', str(raw_column.dtype)):
                self.columns[raw_col_name] = BooleanColumn(raw_column, self.name)
    
    def get_summary(self):
        return {
            'path': self.path,
            'format': self.input_format,
            'size': self.size,
            'columns': self.columns,
            'name': self.name,
            'load_time': self.load_time
        }
        
    def get_cols_oftype(self, data_type):
        string_aliases = ['object', 'str', 'o']
        numeric_aliases = ['number', 'n']
        temporal_aliases = ['time', 'datetime', 'date', 't'],
        boolean_aliases = ['bool', 'b']
        
        if data_type in string_aliases:
            data_type = 'string'
        elif data_type in numeric_aliases:
            data_type = 'numeric'
        elif data_type in temporal_aliases:
            data_type = 'temporal'
        elif data_type in boolean_aliases:
            data_type = 'boolean'
            
        cols_oftype = {}
        for col_name, col in self.columns.items():
            if data_type.lower() in col.data_type.lower():
                cols_oftype[col_name] = col
        
        return cols_oftype
                
class Column(object):
    def __init__(self, raw_column, name):
        self.name = name
        self.count = raw_column.count()
        self.missing = raw_column.isnull().sum()
        self.data = raw_column
        self.invalid = 0
  
    def __eq__(self, other_col):
        return other_col.__class__ == self.__class__


class StringColumn(Column):
    def __init__(self, raw_column, name):
        Column.__init__(self, raw_column, name)
        self.data_type = self.__class__.__name__
        self.text_length_mean = raw_column.str.len().mean()
        self.text_length_std = raw_column.str.len().std()
        self.text_length_med = raw_column.str.len().median()
        self.unique = raw_column.nunique()
        self.duplicates = self.count - self.unique
        self.top = raw_column.value_counts().idxmax()

    def get_summary(self) -> dict:
        return {
            'name': self.name,
            'count': self.count,
            'missing': self.missing,
            'data_type': self.data_type,
            'text_length_mean': self.text_length_mean,
            'text_length_std': self.text_length_std,
            'unique': self.unique,
            'duplicates': self.duplicates,
            'top': self.top
        }

    def perform_check(self) -> dict:
        return check_string_column(self)


class NumericColumn(Column):
    def __init__(self, raw_column, name):
        Column.__init__(self, raw_column, name)
        self.data_type = self.__class__.__name__
        self.min = raw_column.min()
        self.max = raw_column.max()
        self.std = raw_column.std()
        self.mean = raw_column.mean()
        self.zeros = (raw_column == 0).sum()

    def get_summary(self) -> dict:
        return {
            'name': self.name,
            'count': self.count,
            'missing': self.missing,
            'data_type': self.data_type,
            'min': self.min,
            'max': self.max,
            'std': self.std,
            'mean': self.mean,
            'zeros': self.zeros
        }

    def perform_check(self) -> dict:
        return check_numeric_column(self)


class TemporalColumn(Column):
    def __init__(self, raw_column, name):
        Column.__init__(self, raw_column, name)
        self.data_type = self.__class__.__name__
        self.min = raw_column.min()
        self.max = raw_column.max()
        descr = raw_column.describe()
        self.unique = descr['unique']
        self.top = descr['top']

    def get_summary(self) -> dict:
        return {
            'name': self.name,
            'count': self.count,
            'missing': self.missing,
            'data_type': self.data_type,
            'min': self.min,
            'max': self.max,
            'unique': self.unique,
            'top': self.top
        }
        
    def perform_check(self) -> dict:
        return check_temporal_column(self)


class BooleanColumn(Column):
    def __init__(self, raw_column, name):
        Column.__init__(self, raw_column, name)
        self.data_type = self.__class__.__name__
        self.top = raw_column.value_counts().idxmax()
 
    def get_summary(self) -> dict:
        return {
            'name': self.name,
            'count': self.count,
            'missing': self.missing,
            'data_type': self.data_type,
            'top': self.top
        }

    def perform_check(self) -> dict:
        return check_boolean_column(self)