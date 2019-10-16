"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    data model for data file object

### DEVELOPER NOTES:
"""
import sys
import pandas as pd
from dataset import Dataset

string_checks = {
    'white_space': False,
    'capitalized': False,
    'empty_text': False,
    'odd_text_length_diff': False,
    'missing_text': False
}

def report_string_warnings(column):
    data = column.data
    for index, content in data.iteritems():
        # white space
        if not string_checks['white_space']:
            if row_content != row_content.strip():
                string_checks['white_space'] = True
        # all caps
        if not string_checks['capitalized']:
            if row_content == row_content.upper():
                string_checks['capitalized'] = True
        # empty text
        if not string_checks['empty_text']:
            if not row_content:
                string_checks['empty_text'] = True
        # suspicious difference in text lenght
        if not string_checks['odd_text_length_diff']:
            if len(row_content) > (2 * column.text_length_std):
                string_checks['odd_text_length_diff'] = False
    # missing values
    if column.missing > 0:
        string_checks['missing_text'] = True


data_path = r"H:\repos\data-comparator\test_data\nba-raptor\modern_RAPTOR_by_player.csv"
ds = Dataset(data_path)
ds.prepare_columns()
col = ds.columns['player_name']
data = col.data
