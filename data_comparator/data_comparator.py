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

logging.basicConfig(format='%(asctime)s - %(message)s')

datasets = {}
columns = {}

def load(
    *data_sources,
    data_source_names=None
):
    """
    Load a single data source to add to the set of active datasets
    Parameters:
        data_source: Object in the form of a csv, parquet, or sas path... or
        spark/pandas dataframe
        data_source_name: Custom name for the resulting dataset. Default will
        be provided if null
    Output:
        Resulting dataset
    """
    global datasets

    for i, src in enumerate(data_sources):

        if data_source_names:
            try:
                src_name = data_source_names[i]
            except IndexError:
                logging.error(
                    'Number of names must match number of data sources'
                )
        else:
            src_name = 'dataset_' + str(i)

        logging.info(
            "Creating dataset {} from source {}...".format(src_name, src)
        )

        dataset = Dataset(src, src_name)
        dataset.prepare_columns()
        datasets[src_name] = dataset

        for column in dataset.columns:
            columns[column.__class__.__name__] = column

    return datasets

def compare(col_a, col_b):
    col_a

def clear():
    """Removes all active datasets"""
    logging.info("Clearing all active datasets...")
    global datasets
    datasets = {}

def remove(src_name):
    """
    Removes the specified dataset from active datasets
    Parameters:
        src_name: Name of dataset to remove
    """
    try:
        logging.info('Removing {}'.format(src_name))
        del datasets[src_name]
    except NameError:
        logging.error('Could not find dataset {}'.format(src_name))

def main():
    return 0

if __name__ == '__main__':
    main()
    sys.exit(RETURN_CODE)
