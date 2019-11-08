"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    Main function to house the implementation of data comparator tool

### DEVELOPER NOTES:
"""
# pylint: disable=no-member
import logging
import sys
import pandas as pd
from models.dataset import Dataset
from models.comparison import Comparison

logging.basicConfig(format='%(asctime)s - %(message)s')

_DATASETS = {}
_COMPARISONS = {}
_COMP_DF_DICT = {}
_PROFILE = {}


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
    if data_source:
        src = data_source
    else:
        print('ERROR: Valid data source must be provided')
        return

    if data_source_name:
        src_name = data_source_name
    else:
        dataset_index = len(_DATASETS)
        src_name = 'dataset_' + str(dataset_index)

    print(
        "Creating dataset '{}' from source '{}'...".format(src_name, src)
    )

    dataset = Dataset(src, src_name)
    dataset.prepare_columns()
    _DATASETS[src_name] = dataset

    return dataset


def add_datasets(
        *data_sources,
        data_source_names:tuple=None
    ):
    """
    Load multiple data sources to add to the set of active datasets
    Parameters:
        data_sources: Sequence of objects in the form of a csv, parquet, or sas path... or
        spark/pandas dataframe
        data_source_names: Tuple of custom name for the resulting dataset. Default will
        be provided if null
    Output:
        Resulting dataset collection
    """
    if not data_sources:
        print('ERROR: Valid data source must be provided')
        return

    for i, src in enumerate(data_sources):
        if data_source_names:
            try:
                src_name = data_source_names[i]
            except IndexError:
                print('ERROR: Number of names must match number of data sources')
        else:
            dataset_index = len(_DATASETS)
            src_name = 'dataset_' + str(dataset_index)

        print(
            "Creating dataset '{}' from source '{}'...".format(src_name, src)
        )

        dataset = Dataset(src, src_name)
        dataset.prepare_columns()
        _DATASETS[src_name] = dataset


def clear_datasets():
    """Removes all active datasets"""
    print("Clearing all active datasets...")
    _DATASETS = {}


def remove_dataset(src_name):
    """
    Removes the specified dataset from active datasets
    Parameters:
        src_name: Name of dataset to remove
    """
    try:
        print('Removing {}'.format(src_name))
        del _DATASETS[src_name]
    except NameError:
        print('ERROR: Could not find dataset {}'.format(src_name))


def add_comparison(dataset1, dataset2, col_pair):
    assert dataset1 and isinstance(dataset1.__class__, Dataset.__class__), \
        'ERROR: At least one valid dataset must be provided'
    assert not isinstance(dataset2, tuple), \
        'ERROR: Must enter a second dataset. \
            If only one dataset is needed, enter "None" for the second'
    assert col_pair, 'ERROR: At least one column pair must be provided for comparison'

    ds1 = None
    ds2 = None
    col1 = None
    col2 = None

    ds1 = dataset1
    ds2 = dataset2 if dataset2 else dataset1

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
    
    comp = Comparison(col1, col2)
    _COMPARISONS[comp.name] = comp

    return comp


def add_comparisons(dataset1, dataset2, *col_pairs):
    assert dataset1 and isinstance(dataset1.__class__, Dataset.__class__), \
        'ERROR: At least one valid dataset must be provided'
    assert not isinstance(dataset2, tuple), \
        'ERROR: Must enter a second dataset. \
            If only one dataset is needed, enter "None" for the second'
    assert len(col_pairs) > 0, 'ERROR: At least one column pair must be provided for comparison'

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

        comp = Comparison(col1, col2)
        _COMPARISONS[comp.name] = comp

    return _COMPARISONS


def profile(dataset, col_list):
    assert dataset and isinstance(dataset.__class__, Dataset.__class__), \
        'ERROR: At least one valid dataset must be provided'
    assert columns and isintance(columns, list), \
        'ERROR: Columns must be provided in a list format'

    _PROFILE = {}
    if '*' in columns:
        for col in dataset.columns:
            comp = Comparison(col1, col2)


    


def clear_comparisons():
    """Removes all active copmarisons"""
    print("Clearing all active comparisons...")
    _COMPARISONS = {}


def remove_comparison(comp_name):
    """
    Removes the specified comparison from active datasets
    Parameters:
        comp_name: Name of comparison to remove
    """
    try:
        print('Removing comparison {}'.format(comp_name))
        del _COMPARISONS[comp_name]
    except NameError:
        print('ERROR: Could not find comparison {}'.format(comp_name))
        

def clear_all():
     """Removes all active datasets and copmarisons"""
     clear_datasets()
     clear_comparisons()


def compare():
    for comp in _COMPARISONS.values():
        data = {
            comp.col1.name: list(comp.col1.get_summary().values()),
            comp.col2.name: list(comp.col2.get_summary().values())
        }
        df = pd.DataFrame(data, index=list(comp.col1.get_summary().keys()))
        _COMP_DF_DICT[comp.name] = df


def view(comp_name):
    print(_COMP_DF_DICT[comp_name])


def main():
    return 0


if __name__ == '__main__':
    main()
    sys.exit(0)
