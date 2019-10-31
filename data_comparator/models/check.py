"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    data model for data file object

### DEVELOPER NOTES:
"""

import logging

def check_string_column(column):
    rows = column.data
    string_checks = {
        'white_space': False,
        'capitalized': False,
        'empty_text': False,
        'numeric_data': False,
        'odd_text_length_diff': False,
        'missing_text': False
    }

    for index, row_content in rows.iteritems():
        # check for numeric data
        try:
            float(row_content)
            string_checks['numeric_data'] = True
        except:
            continue
        # white space
        if not string_checks['white_space']:
            if row_content != row_content.strip():
                string_checks['white_space'] = True
        # all caps
        if not string_checks['capitalized']:
            logging.info('check for cap')
            if row_content == row_content.upper():
                string_checks['capitalized'] = True
        # empty text
        if not string_checks['empty_text']:
            if not row_content:
                string_checks['empty_text'] = True
        # suspicious difference in text lenght
        if not string_checks['odd_text_length_diff']:
            if len(row_content) > (2 * column.text_length_std):
                string_checks['odd_text_length_diff'] = True
    # missing values
    if column.missing > 0:
        string_checks['missing_text'] = True

    return string_checks


def check_numeric_column(column):
    pass


def check_temporal_column(column):
    pass


def check_boolean_column(columns):
    pass
