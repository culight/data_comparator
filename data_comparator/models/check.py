"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    data model for data file object

### DEVELOPER NOTES:
"""
import pandas as pd

def check_string_column(column):
    rows = column.data
    string_checks = {
        'white_space': False,
        'capitalized': False,
        'empty_text': False,
        'numeric_data': False,
        'odd_text_length_diff': False
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
        except:
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
                if row_content and len(row_content) > 0:
                    if row_content == row_content.upper():
                        string_checks['capitalized'] = True
            # suspicious difference in text length
            if not string_checks['odd_text_length_diff']:
                diff = abs(len(row_content) - column.text_length_mean)
                if diff > (2 * column.text_length_med):
                    string_checks['odd_text_length_diff'] = True

    return string_checks


def check_numeric_column(column):
    pass


def check_temporal_column(column):
    pass


def check_boolean_column(columns):
    pass
