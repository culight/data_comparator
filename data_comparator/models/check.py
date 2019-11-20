"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    data model for data file object

### DEVELOPER NOTES:
"""
import re
import pandas as pd
import numpy as np
from dateutil.parser import parse

def check_string_column(column):
    rows = column.data
    string_checks = {
        'white_space': False,
        'capitalized': False,
        'empty_text': False,
        'numeric_data': False,
        'odd_text_length_diff': False,
        'special_char': False,
        'temporal_data': False
    }

    print('\nPerforming check for string column...')
    for index, row_content in rows.iteritems():
        skip=False
        # check for missing data
        if pd.isnull(row_content):
            continue
            
        # check for numeric data
        try:
            float(row_content)
            string_checks['numeric_data'] = True
            skip = True
        except ValueError:
            pass

        # check for time data
        try:
            parse(row_content, fuzzy=True)
            string_checks['temporal_data'] = True
            skip = True
        except ValueError:
            pass

        # empty text
        if len(row_content.replace(' ','')) == 0:
            string_checks['empty_text'] = True
            skip=True
        
        if not skip:
            # white space
            if not string_checks['white_space']:
                if row_content != row_content.replace(' ', ''):
                    string_checks['white_space'] = True
                    
            # all caps
            if not string_checks['capitalized']:
                if re.search(r'^[a-zA-Z]$', row_content) and row_content == row_content.upper():
                    string_checks['capitalized'] = True
                    
            # special characters
            if not string_checks['special_char']:
                if re.search(r'[.!@#$%&*_+-=|\:";<>,./()[\]{}\'].', row_content):
                    string_checks['special_char'] = True
                    
            # suspicious difference in text length
            if not string_checks['odd_text_length_diff']:
                diff = abs(len(row_content) - column.text_length_mean)
                if diff > (2 * column.text_length_med):
                    string_checks['odd_text_length_diff'] = True

    return string_checks


def check_numeric_column(column):
    numeric_checks = {
        'pot_outliers': False,
        'susp_skewness': False,
        'susp_zero_count': False,
    }

    col_skew = column.data.skew()
    if (col_skew < -1) | (col_skew > 1):
        numeric_checks['susp_skewness'] = True

    col_zscore = (column.data - column.data.mean())/column.data.std(ddof=0)
    num_pot_outliers = len(np.where(np.abs(col_zscore) > 3)[0])
    if(num_pot_outliers > 0):
        numeric_checks['pot_outliers'] = True

    zero_perc = column.zeros/column.count
    if zero_perc > 0.15:
        numeric_checks['susp_zero_count'] = True

    return numeric_checks


def check_temporal_column(column):
    temporal_checks = {
        'empty_date'
        'small_range',
        'large_range'
    }

    # check for empty fields
    temporal_checks['empty_date'] = date.data.empty 
    
    # check for odd ranges
    if column.max() - column.min() < 90:
        temporal_checks['small_range'] = True

    # check for odd ranges
    if column.max() - column.min() > 365*5:
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