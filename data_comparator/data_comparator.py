"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    Main function to house the implementation of data comparator tool

### DEVELOPER NOTES:
"""
# pylint: disable=no-member
import logging
import os
from models.dataset import Dataset
import models.check

logging.basicConfig(format='%(asctime)s - %(message)s')

datasets = {}


def perform_column_checks():
    for dataset in datasets.values:
        if dataset:
            for column in dataset.columns:
                pass

def load(
    src1,
    src2=None,
    src1_name='data_source_1',
    src2_name='data_source_2'
):
    global datasets

    logging.info('Loading data sources...')
    dataset_1 = Dataset(src1, src1_name)
    dataset_1.prepare_columns()
    datasets[src1_name] = dataset_1

    if src2:
        dataset_2 = Dataset(src2, src2_name)
        dataset_2.prepare_columns()
        datasets[src2_name] = dataset_2

    return datasets

def clear():
    global datasets
    datasets = {}

def remove(src_name):
    try:
        del datasets[src_name]
    except NameError:
        logging.error('Could not find dataset {}'.format(src_name))

def compare():
    pass

def main():
    return 0

if __name__ == '__main__':
    main()
    sys.exit(RETURN_CODE)
