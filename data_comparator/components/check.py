"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    data model for data file object

### DEVELOPER NOTES:
"""
import re
import string
import pandas as pd
import numpy as np
from dateutil.parser import parse

def check_string_column(column, row_limit):
    rows = column.data
    string_checks = {
        'white_space': '',
        'capitalized': '',
        'empty_text': '',
        'numeric_data': '',
        'odd_text_length_diff': '',
        'special_char': '',
        'temporal_data': '',
        'bytes_data': ''
    }
    spec_chars = set(string.punctuation)

    print('\nPerforming check for string column...')
    rows = rows if row_limit == -1 else rows[0:row_limit]
    for index, row_content in rows.iteritems():
        skip=False
        # check for missing data
        if pd.isnull(row_content):
            continue
        
        # check for byte type
        if type(row_content) == bytes:
            string_checks['bytes_data'] = row_content
            row_content = row_content.decode()
            
        # check for numeric data
        if not re.search(r'.[a-zA-Z].', row_content):
            try:
                float(row_content)
                string_checks['numeric_data'] = row_content
                skip = True
            except ValueError:
                pass

        # check for time data
        try:
            parse(row_content)
            string_checks['temporal_data'] = row_content
            skip = True
        except ValueError:
            pass

        # empty text
        if len(row_content.replace(' ','')) == 0:
            string_checks['empty_text'] = index
            skip=True
        
        if not skip:
            # white space
            if not string_checks['white_space']:
                if row_content != row_content.replace(' ', ''):
                    string_checks['white_space'] = row_content
                    
            # all caps
            if not string_checks['capitalized']:
                if row_content == row_content.upper():
                    string_checks['capitalized'] = row_content
                    
            # special characters
            if not string_checks['special_char']:
                if any(char in spec_chars for char in row_content):
                    string_checks['special_char'] = row_content
                    
            # suspicious difference in text length
            if not string_checks['odd_text_length_diff']:
                diff = abs(len(row_content) - column.text_length_mean)
                if diff > (2 * column.text_length_med):
                    string_checks['odd_text_length_diff'] = row_content

    return string_checks


def check_numeric_column(column):
    numeric_checks = {
        'pot_outliers': '',
        'susp_skewness': '',
        'susp_zero_count': '',
    }

    col_skew = column.data.skew()
    if (col_skew < -1) | (col_skew > 1):
        numeric_checks['susp_skewness'] = str(col_skew)

    col_zscore = (column.data - column.data.mean())/column.data.std(ddof=0)
    num_pot_outliers = len(np.where(np.abs(col_zscore) > 3)[0])
    if(num_pot_outliers > 0):
        numeric_checks['pot_outliers'] = str(num_pot_outliers)

    zero_perc = column.zeros/column.count
    if zero_perc > 0.15:
        numeric_checks['susp_zero_count'] = str(zero_perc)

    return numeric_checks


def check_temporal_column(column):
    temporal_checks = {
        'empty_date': False,
        'small_range': False,
        'large_range': False
    }

    # check for empty fields
    temporal_checks['empty_date'] = column.data.empty 
    
    time_delta = column.max - column.min
    
    # check for odd ranges
    if time_delta.days < 90:
        temporal_checks['small_range'] = True

    # check for odd ranges
    if time_delta.days > 365*5:
        temporal_checks['large_range'] = True

    return temporal_checks


def check_boolean_column(column):
    boolean_checks = {
        'only_true': False,
        'only_false': False
    }
    
    print('\nPerforming check for string column...')
    if (column.top == False) and (column.unique == 1):
        boolean_checks['only_false'] == True
    elif (column.top == True) and (column.unique == 1):
        boolean_checks['only_true'] == True
        
    return boolean_checks