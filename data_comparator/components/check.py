"""
### CODE OWNERS: Demerrick Moton
### OBJECTIVE:
    Module for the data type checks
### DEVELOPER NOTES:
"""
import re
import os
import string
import pandas as pd
import numpy as np
from dateutil.parser import parse
import logging

logging.basicConfig(
    format="%(asctime)s - %(message)s", level=os.environ.get("LOGLEVEL", "INFO")
)
LOGGER = logging.getLogger(__name__)
# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def check_string_column(column, validations, row_limit=None):
    rows = column.data

    string_checks = {}
    for case, settings in validations.items():
        doCheck = (len(settings["fields"]) == 0) or ("name" in settings["fields"])
        if settings["enabled"] and doCheck:
            string_checks[case] = ""

    spec_chars = set(string.punctuation)

    LOGGER.info("Performing check for string column...")
    rows = rows if not row_limit else rows[0:row_limit]
    for index, row_content in rows.iteritems():
        skip = False
        # check for missing data
        if pd.isnull(row_content):
            continue

        # check for byte type
        if type(row_content) == bytes:
            # string_checks["bytes_data"] = row_content
            row_content = row_content.decode()

        # check for numeric data
        if not re.search(r".[a-zA-Z].", str(row_content)):
            try:
                float(row_content)
                string_checks["numeric_data"] = row_content
                skip = True
            except Exception:
                pass

        # check for time data
        try:
            parse(row_content)
            # string_checks["temporal_data"] = row_content
            skip = True
        except Exception:
            pass

        # empty text
        if len(row_content.replace(" ", "")) == 0:
            string_checks["empty_text"] = index
            skip = True

        if not skip:  # optional checks below
            # white space
            if "white_space" in string_checks:
                if not string_checks["white_space"]:
                    if row_content != row_content.replace(" ", ""):
                        string_checks["white_space"] = row_content

            # all caps
            if "capitalized" in string_checks:
                if not string_checks["capitalized"]:
                    if row_content == row_content.upper():
                        string_checks["capitalized"] = row_content

            # special characters
            if "special_char" in string_checks:
                if not string_checks["special_char"]:
                    if any(char in spec_chars for char in row_content):
                        string_checks["special_char"] = row_content

            # suspicious difference in text length
            if "odd_text_length_diff" in string_checks:
                if not string_checks["odd_text_length_diff"]:
                    diff = abs(len(row_content) - column.text_length_mean)
                    if diff > (2 * column.text_length_med):
                        string_checks["odd_text_length_diff"] = row_content

            if "field_length" in string_checks:
                if not string_checks["field_length"]:
                    if len(row_content) != validations["field_length"]["value"]:
                        string_checks["field_length"] = row_content

            if "contains" in string_checks:
                if not string_checks["contains"]:
                    if row_content in validations["contains"]["value"].split(","):
                        string_checks["contains"] = row_content

    return string_checks


def check_numeric_column(column, validations):
    df = column.data.to_frame()

    LOGGER.info("Performing check for numeric column...")
    numeric_checks = {}
    for case, settings in validations.items():
        doCheck = (len(settings["fields"]) == 0) or ("name" in settings["fields"])
        if settings["enabled"] and doCheck:
            numeric_checks[case] = ""

    col_skew = column.data.skew()
    if "susp_skewness" in numeric_checks:
        if (col_skew < -1) | (col_skew > 1):
            numeric_checks["susp_skewness"] = str(col_skew)

    if "pot_outliers" in numeric_checks:
        col_zscore = (column.data - column.data.mean()) / column.data.std(ddof=0)
        num_pot_outliers = len(np.where(np.abs(col_zscore) > 3)[0])
        if num_pot_outliers > 0:
            numeric_checks["pot_outliers"] = str(num_pot_outliers)

    if "susp_zero_count" in numeric_checks:
        zero_perc = column.zeros / column.count
        if zero_perc > 0.15:
            numeric_checks["susp_zero_count"] = str(zero_perc)

    if "value_threshold_upper" in numeric_checks:
        value = None
        try:
            value = df[
                df[column.name] > float(validations["value_threshold_upper"]["value"])
            ].iloc[0][0]
        except IndexError:
            pass  # add logger
        if value:
            numeric_checks["value_threshold_upper"] = str(value)

    if "value_threshold_lower" in numeric_checks:
        value = None
        try:
            value = df[
                df[column.name] < float(validations["value_threshold_lower"]["value"])
            ].iloc[0][0]
        except IndexError:
            pass  # add logger
        if value:
            numeric_checks["value_threshold_upper"] = str(value)

    if "value_threshold_stdev" in numeric_checks:
        pass

    return numeric_checks


def check_temporal_column(column, validations):

    LOGGER.info("Performing check for temporal (time, datatime, etc.) column...")
    temporal_checks = {}
    for case, settings in validations.items():
        doCheck = (len(settings["fields"]) == 0) or ("name" in settings["fields"])
        if settings["enabled"] and doCheck:
            temporal_checks[case] = ""

    # check for empty fields
    if "empty_date" in temporal_checks:
        temporal_checks["empty_date"] = column.data.empty

    time_delta = column.max - column.min

    # check for odd ranges
    if "small_range" in temporal_checks:
        if time_delta.days < 90:
            temporal_checks["small_range"] = True

    # check for odd ranges
    if "large_range" in temporal_checks:
        if time_delta.days > 365 * 5:
            temporal_checks["large_range"] = True

    return temporal_checks


def check_boolean_column(column, validations):
    LOGGER.info("Performing check for boolean column {}...".format(column.name))
    boolean_checks = {}
    for case, settings in validations.items():
        doCheck = (len(settings["fields"]) == 0) or ("name" in settings["fields"])
        if settings["enabled"] and doCheck:
            boolean_checks[case] = ""

    if "only_false" in boolean_checks:
        if (column.top == False) and (column.unique == 1):
            boolean_checks["only_false"] = True
    if "only_true" in boolean_checks:
        if (column.top == True) and (column.unique == 1):
            boolean_checks["only_true"] = True

    return boolean_checks
