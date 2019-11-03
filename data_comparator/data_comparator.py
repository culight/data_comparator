"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    Main function to house the implementation of data comparator tool

### DEVELOPER NOTES:
"""
# pylint: disable=no-member
import logging
import os
import sys
from models.dataset import Dataset
from models.comparison import Comparison

logging.basicConfig(format='%(asctime)s - %(message)s')

datasets = {}
comparisons = {}


def add_dataset(
    data_source,
    data_source_name=''
):
    """
    Load a single data source to add to the set of active datasets
    Parameters:
        data_source: Object in the form of a csv, parquet, or sas path... or
        spark/pandas dataframe
        data_source_name: Custom name for the resulting dataset. Default will
        be provided if null
    Output:
        Resulting dataset collection
    """
    global datasets
    
    if data_source:
        src = data_source
    else:
        logging.error('Valid data source must be provided')

    if data_source_name:
        src_name = data_source_name
    else:
        dataset_index = len(datasets)
        src_name = 'dataset_' + str(dataset_index)

    print(
        "Creating dataset '{}' from source '{}'...".format(src_name, src)
    )

    dataset = Dataset(src, src_name)
    dataset.prepare_columns()
    datasets[src_name] = dataset

    return dataset


def add_datasets(
    *data_sources,
    data_source_names:list=None
):
    """
    Load multiple data sources to add to the set of active datasets
    Parameters:
        data_sources: Sequence of objects in the form of a csv, parquet, or sas path... or
        spark/pandas dataframe
        data_source_names: List of custom name for the resulting dataset. Default will
        be provided if null
    Output:
        Resulting dataset collection
    """    
    if not data_sources:
        logging.error('Valid data source must be provided')

    global datasets
    
    for i, src in enumerate(data_sources):
        if data_source_names:
            try:
                src_name = data_source_names[i]
            except IndexError:
                print('Number of names must match number of data sources')
        else:
            dataset_index = len(datasets)
            src_name = 'dataset_' + str(dataset_index)

        print(
            "Creating dataset '{}' from source '{}'...".format(src_name, src)
        )

        dataset = Dataset(src, src_name)
        dataset.prepare_columns()
        datasets[src_name] = dataset


def clear_datasets():
    """Removes all active datasets"""
    print("Clearing all active datasets...")
    global datasets
    datasets = {}


def remove_dataset(src_name):
    """
    Removes the specified dataset from active datasets
    Parameters:
        src_name: Name of dataset to remove
    """
    global datasets
    try:
        print('Removing {}'.format(src_name))
        del datasets[src_name]
    except NameError:
        logging.error('Could not find dataset {}'.format(src_name))


def add_comparisons(dataset1, dataset2, *col_pairs):
    assert dataset1 and isinstance(dataset1.__class__, Dataset.__class__), \
        'ERROR: At least one valid dataset must be provided'
    assert not isinstance(dataset2, tuple), \
        'ERROR: Must enter a second dataset. \
            If only one dataset is needed, enter "None" for the second'
    assert len(col_pairs) > 0, 'ERROR: At least one column pair must be provided for comparison'
        
    global comparisons
        
    ds1 = None
    ds2 = None
    col1 = None
    col2 = None
    
    ds1 = dataset1
    ds2 = dataset2 if dataset2 else dataset1
    
    for col_pair in col_pairs:
        assert isinstance(col_pair, tuple) and len(col_pair) == 2, \
            'ERROR: Column pairing must be presented as a tuple of two columns to be compared'

        if col_pair[0] in ds1.columns:
            col1 = ds1.columns[col_pair[0]]
        else:
            print('ERROR: {} is not a column in {}'.format(col_pair[0], ds1.name))
            return
        if col_pair[1] in ds2.columns:
            col2 = ds2.columns[col_pair[1]]
        else:
            print('ERROR: {} is not a column in {}'.format(col_pair[1], ds2.name))
            return
        
        # try:
        comp = Comparison(col1, col2)
        comparisons[comp.name] = comp
        # except:
        #     print(
        #         'ERROR: Problem encountered creating {} and {} comparison'.format(col1, col2)
        #     )

    return comparisons


def clear_comparisons():
    """Removes all active copmarisons"""
    print("Clearing all active comparisons...")
    global comparisons
    comparisons = {}


def remove_comparison(comp_name):
    """
    Removes the specified comparison from active datasets
    Parameters:
        comp_name: Name of comparison to remove
    """
    try:
        print('Removing comparison {}'.format(comp_name))
        del comparisons[comp_name]
    except NameError:
        print('Could not find comparison {}'.format(comp_name))
        

def clear_all():
     """Removes all active datasets and copmarisons"""
     clear_datasets()
     clear_comparisons()


def compare():
    pass


def view_results():
    pass


def main():
    return 0


if __name__ == '__main__':
    main()
    sys.exit(0)
