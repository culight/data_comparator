"""
### CODE OWNERS: Demerrick Moton
### OBJECTIVE:
    Data model for dataset object
### DEVELOPER NOTES:
"""
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
import re
import json

import pandas as pd

from .check import (
    check_string_column,
    check_numeric_column,
    check_boolean_column,
    check_temporal_column,
)

ACCEPTED_INPUT_FORMATS = [
    "sas7bdat",
    "csv",
    "parquet",
    "pyspark",
    "pandas",
    "json",
    "txt",
]
COMP_DIR = Path(__file__).parent
VALID_FILE = str(COMP_DIR / "validations_config.json")

logging.basicConfig(
    stream=sys.stdout, format="%(asctime)s - %(message)s", level=logging.DEBUG
)
LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class Dataset(object):
    def __init__(self, data_src: object, name: str, **load_params):
        self.path = None
        self.input_format = ""
        self.size = ""
        self.columns = {}
        self.dataframe = None
        self.name = name
        self.load_time = 0.0
        try:
            # probably a path string
            self.path = Path(data_src)
            self.input_format = self._get_input_format()
            self.size = self._get_data_size(data_src, **load_params)
            self.dataframe = self._load_data_frompath(**load_params)
        except TypeError:
            # probably an dataframe object
            self.input_format = str(data_src.__class__)
            if "DataFrame" in self.input_format:
                self.dataframe = self._load_data_fromdf(data_src)
                # count object types in size
                self.size = self.dataframe.memory_usage(deep=True).sum()

        self._prepare_columns()

    def __getitem__(self, item):
        try:
            col = self.columns[item]
            return col
        except KeyError:
            LOGGER.error("Item {} was not found".format(item))

    def __delitem__(self, key):
        del self.columns[key]

    def __iter__(self):
        return iter(self.columns)

    def __len__(self):
        len(self.columns)

    def __name__(self):
        return self.name

    @classmethod
    def to_yaml(cls, representer, node):
        tag = getattr(cls, "yaml_tag", "!" + cls.__name__)

    @classmethod
    def from_yaml(cls, constructor, node):
        return cls(*node.value.split("-"))

    def _get_input_format(self) -> str:
        suffix = self.path.suffix.replace(".", "")
        if suffix not in ACCEPTED_INPUT_FORMATS:
            raise ValueError("File type not supported")
        return suffix

    def _format_size(self, size):
        "Return the given bytes as a human friendly KB, MB, GB, or TB string"
        B = float(size)
        KB = float(1024)
        MB = float(KB ** 2)  # 1,048,576
        GB = float(KB ** 3)  # 1,073,741,824
        TB = float(KB ** 4)  # 1,099,511,627,776

        if size < KB:
            return "{0} {1}".format(B, "Bytes" if 0 == size > 1 else "Byte")
        elif KB <= size < MB:
            return "{0:.2f} KB".format(size / KB)
        elif MB <= size < GB:
            return "{0:.2f} MB".format(size / MB)
        elif GB <= size < TB:
            return "{0:.2f} GB".format(size / GB)
        elif TB <= size:
            return "{0:.2f} TB".format(size / TB)

        return size

    def _get_data_size(self, data_src: object, **load_params) -> int:
        size = 0
        if self.path.is_dir():
            for file in os.listdir(data_src):
                abs_path = os.path.join(data_src, file)
                size += os.path.getsize(abs_path)
        else:
            size = os.path.getsize(data_src)
        if size < 1:
            raise ValueError("File size of {} is too small".format(size))
        if size > 800000000 and "chunksize" in list(load_params.keys()):
            chunksize = load_params["chunksize"]
            if chunksize > 200000000:
                raise ValueError("chunksize {} is too large".format(chunksize))
        elif size > 800000000:
            raise ValueError("File size of {} is too large".format(size))

        formatted_size = self._format_size(size)
        return formatted_size

    def _load_data_frompath(self, **load_params) -> pd.DataFrame:
        LOGGER.debug("\nLoading raw data into dataset object...")
        data = None
        start_time = datetime.now()
        if self.input_format == "sas7bdat":
            data = pd.read_sas(str(self.path), **load_params)
        elif self.input_format == "csv":
            data = pd.read_csv(str(self.path), **load_params)
        elif self.input_format == "txt":
            if "sep" not in list(load_params.keys()):
                raise ValueError("Please provide a valid delimiter for this text file")
            data = pd.read_table(str(self.path), **load_params)
        elif self.input_format == "parquet":
            data = pd.read_parquet(str(self.path), engine="pyarrow", **load_params)
        elif self.input_format == "json":
            data = pd.read_json(str(self.path), **load_params)
        else:
            raise ValueError("Path type {} not recognized".format(self.input_format))
        end_time = datetime.now()
        self.load_time = str(end_time - start_time)
        return data

    def _load_data_fromdf(self, df) -> pd.DataFrame:
        LOGGER.debug("\nLoading raw data into dataset object...")
        data = None
        start_time = datetime.now()
        if "pyspark" in self.input_format:
            data = df.toPandas()
        elif "pandas" in self.input_format:
            data = df
        else:
            raise ValueError("object type not recognized")
        end_time = datetime.now()
        self.load_time = str(end_time - start_time)
        return data

    def convert_dates(self, raw_column):
        if (raw_column.dtype == "object") or (raw_column.dtype == "O"):
            try:
                raw_column = pd.to_datetime(raw_column)
            except (ValueError, TypeError, AttributeError) as e:
                pass
            except OverflowError:
                raw_column = raw_column.astype("str")
        return raw_column

    def _prepare_columns(self):
        LOGGER.debug("\nPreparing columns...")
        if len(self.dataframe.columns) == 0:
            raise TypeError("No columns found for this dataframe")
        for raw_col_name in self.dataframe.columns:
            LOGGER.debug(raw_col_name)
            raw_column = self.convert_dates(self.dataframe[raw_col_name])
            if re.search(r"(int)", str(raw_column.dtype)):
                self.columns[raw_col_name] = NumericColumn(raw_column, self.name)
            elif re.search(r"(float)", str(raw_column.dtype)):
                self.columns[raw_col_name] = NumericColumn(raw_column, self.name)
            elif re.search(r"(str)", str(raw_column.dtype)):
                self.columns[raw_col_name] = StringColumn(raw_column, self.name)
            elif re.search(r"time|ns", str(raw_column.dtype)):
                self.columns[raw_col_name] = TemporalColumn(raw_column, self.name)
            elif re.search(r"(bool)", str(raw_column.dtype)):
                self.columns[raw_col_name] = BooleanColumn(raw_column, self.name)
            else:
                if raw_column.notna().dtype == "bool":
                    self.columns[raw_col_name] = BooleanColumn(raw_column, self.name)
                self.columns[raw_col_name] = StringColumn(raw_column, self.name)

    def get_summary(self):
        return {
            "path": self.path,
            "format": self.input_format,
            "size": self.size,
            "columns": self.columns,
            "ds_name": self.name,
            "load_time": self.load_time,
        }

    def get_cols_oftype(self, data_type):
        string_aliases = ["object", "str", "o"]
        numeric_aliases = ["number", "n", "int"]
        temporal_aliases = ["time", "datetime", "date", "t"]
        boolean_aliases = ["bool", "b"]

        if data_type in string_aliases:
            data_type = "string"
        elif data_type in numeric_aliases:
            data_type = "numeric"
        elif data_type in temporal_aliases:
            data_type = "temporal"
        elif data_type in boolean_aliases:
            data_type = "boolean"

        cols_oftype = {}
        for col_name, col in self.columns.items():
            if data_type.lower() in col.data_type.lower():
                cols_oftype[col_name] = col

        return cols_oftype


class Column(object):
    def __init__(self, raw_column, ds_name):
        self.ds_name = ds_name
        self.count = raw_column.count()
        self.missing = raw_column.isnull().sum()
        self.data = raw_column
        self.invalid = 0
        self.name = raw_column.name

    def __eq__(self, other_col):
        return other_col.__class__ == self.__class__

    def load_validation_settings(self):
        validation_data = None
        if not validation_data:
            with open(VALID_FILE, "r") as read_file:
                validation_data = json.load(read_file)

        assert validation_data, LOGGER.error(
            "Error encountered while loading validations"
        )

        return validation_data["type"]


class StringColumn(Column):
    def __init__(self, raw_column, ds_name):
        try:
            Column.__init__(self, raw_column, ds_name)
            self.data_type = self.__class__.__name__
            self.text_length_mean = raw_column.str.len().mean()
            self.text_length_std = raw_column.str.len().std()
            self.text_length_med = raw_column.str.len().median()
            descr = raw_column.describe()
            self.unique = descr["unique"]
            self.duplicates = self.count - self.unique
        except Exception as e:
            LOGGER.error(e)
            # TODO: add to list of uncon. types

    def get_summary(self) -> dict:
        return {
            "ds_name": self.ds_name,
            "name": self.name,
            "count": self.count,
            "missing": self.missing,
            "data_type": self.data_type,
            "text_length_mean": self.text_length_mean,
            "text_length_std": self.text_length_std,
            "unique": self.unique,
            "duplicates": self.duplicates,
            # 'top': self.top
        }

    def perform_check(self, row_limit=-1) -> dict:
        validation_settings = self.load_validation_settings()
        return check_string_column(self, validation_settings["string"], row_limit)


class NumericColumn(Column):
    def __init__(self, raw_column, ds_name):
        Column.__init__(self, raw_column, ds_name)
        self.data_type = self.__class__.__name__
        self.min = raw_column.min()
        self.max = raw_column.max()
        self.std = raw_column.std()
        self.mean = raw_column.mean()
        self.zeros = (raw_column == 0).sum()

    def get_summary(self) -> dict:
        return {
            "ds_name": self.ds_name,
            "name": self.name,
            "count": self.count,
            "missing": self.missing,
            "pct_missing": round(float(self.missing / self.count) * 100, 2),
            "data_type": self.data_type,
            "mean": self.mean,
            "min": self.min,
            "max": self.max,
            "std": self.std,
            "zeros": self.zeros,
            "pct_zero": round(float(self.zeros / self.count) * 100, 2),
        }

    def perform_check(self) -> dict:
        validation_settings = self.load_validation_settings()
        return check_numeric_column(self, validation_settings["numeric"])


class TemporalColumn(Column):
    def __init__(self, raw_column, ds_name):
        Column.__init__(self, raw_column, ds_name)
        self.data_type = self.__class__.__name__
        self.min = raw_column.min()
        self.max = raw_column.max()
        self.unique = len(raw_column)-len(raw_column.drop_duplicates())

    def get_summary(self) -> dict:
        return {
            "ds_name": self.ds_name,
            "name": self.name,
            "count": self.count,
            "missing": self.missing,
            "data_type": self.data_type,
            "min": self.min,
            "max": self.max,
            "unique": self.unique,
        }

    def perform_check(self) -> dict:
        validation_settings = self.load_validation_settings()
        return check_temporal_column(self, validation_settings["temporal"])


class BooleanColumn(Column):
    def __init__(self, raw_column, ds_name):
        Column.__init__(self, raw_column, ds_name)
        self.data_type = self.__class__.__name__
        # self.top = raw_column.value_counts().idxmax()

    def get_summary(self) -> dict:
        return {
            "ds_name": self.ds_name,
            "name": self.name,
            "count": self.count,
            "missing": self.missing,
            "data_type": self.data_type,
            # top': self.top
        }

    def perform_check(self) -> dict:
        validation_settings = self.load_validation_settings()
        return check_boolean_column(self, validation_settings["boolean"])
