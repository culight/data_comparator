"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    data model for data file object

### DEVELOPER NOTES:
"""
import sys
import logging
import os
from pathlib import Path

import pandas as pd

logging.basicConfig(format='%(asctime)s - %(message)s')

ACCEPTED_DATA_TYPES = ['sas7bdat', 'csv', 'parquet', 'pyspark', 'pandas']

class Comparable:
    path = Path()
    type = ''
    size = ''
    is_valid = False
    dataframe = None

    def __init__(self, data_src):
        if not data_src:
            logging.error('reference to data source is missing or invalid')
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
                self.dataframe = load_data_fromdf(
                    self.path,
                    self.type
                )
            else:
                logging.error('data type not recognized1')


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
            size = os.path.getsize(data_src) >> 20
        if size < 1:
            logging.error('file is too small')
        return size


    def _load_parquet(self, path):
        return pd.read_parquet(str(path))


    def _load_csv(self, path):
        return pd.read_csv(str(path))


    def _load_pyspark(self, df):
        return dataframe.to_pandas(df)


    def load_data_frompath(self, path, type):
        if type == 'sas7bdat':
            data = pd.read_sas(str(path))
        elif type == 'csv':
            data = _load_csv(path)
        elif type == 'parquet':
            data = _load_parquet(path)
        else:
            logging.error('data type not recognized2')
        return data


    def load_data_fromdf(self, df, type):
        if type == 'pyspark':
            data = _load_pyspark(data_src)
        elif type == 'pandas':
            data = df
        else:
            logging.error('data type not recognized3')
        return data
