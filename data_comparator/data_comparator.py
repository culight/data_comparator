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
from models.dataset import Dataset, Column
from models.comparison import Comparison

logging.basicConfig(format='%(asctime)s - %(message)s')

_DATASETS = {}
_COMPARISONS = {}
_COMP_DF = {}
_PROFILE = {}

def load_dataset(
        data_source,
        data_source_name: str='',
        **load_params
    ):
    """
    Load a single data source to add to the set of saved datasets
    Parameters:
        data_source: Object in the form of a csv, parquet, or sas path... or
        spark/pandas dataframe
        data_source_name: Custom name for the resulting dataset. Default will
        be provided if null
    Output:
        Resulting dataset collection
    """
    assert data_source, 'Data source not provided'

    src = data_source

    if data_source_name:
        src_name = data_source_name
    else:
        dataset_index = len(_DATASETS)
        src_name = 'dataset_' + str(dataset_index)

    print(
        "\nCreating dataset '{}' from source:\n '{}'".format(src_name, src)
    )
    
    ret_dataset = _recycle_dataset(data_source, **load_params)
    if ret_dataset != None:
        _DATASETS[src_name] = ret_dataset
        return ret_dataset

    dataset = Dataset(
        data_src=src, 
        name=src_name, 
        **load_params
    )
    _DATASETS[src_name] = dataset
    
    print("\nDone")

    return dataset


def load_datasets(
        *data_sources,
        data_source_names: list=None,
        load_params_list: list=None
    ):
    """
    Load multiple data sources to add to the set of saved datasets
    Parameters:
        data_sources: Sequence of objects in the form of a csv, parquet, or sas path... or
            spark/pandas dataframe
        data_source_names: Tuple of custom name for the resulting dataset. Default will
            be provided if null
        load_params_list: list of load parameters for each dataset \
            e.g. [{'cols': ['col1', col2']}, {}]
    Output:
        Resulting datasets
    """
    assert data_sources, 'Valid data source must be provided'
    src_names = []
    for i, src in enumerate(data_sources):
        src_name = None
        load_params = None
        dataset = None
        if data_source_names:
            try:
                src_name = data_source_names[i]
            except IndexError:
                print('Number of names must match number of data sources')
        else:
            dataset_index = len(_DATASETS)
            src_name = 'dataset_' + str(dataset_index)
            src_names.append(src_name)

        if load_params_list:
            try:
                load_params = load_params_list[i]
            except IndexError:
                print('Number of load parameters must match number of data sources')
            dataset = Dataset(
                data_src=src, 
                name=src_name,
                **load_params
            )            
        else:
            dataset = Dataset(
                data_src=src, 
                name=src_name
            )

        print("\nCreating dataset '{}'".format(src_name))
        _DATASETS[src_name] = dataset
        print('\nDone')
    
    if not data_source_names:
        data_source_names = src_names

    return [_DATASETS[ds_name] for ds_name in data_source_names]


def get_datasets():
    """
    Return all saved datasets
    Parameters:
    Output:
        All saved datasets
    """
    return _DATASETS


def get_dataset(ds_name):
    """
    Return a particular dataset
    Parameters: 
        ds_name: dataset name
    Output:
        The specified dataset
    """
    return _DATASETS[ds_name]


def _recycle_dataset(data_src, **load_params):
    if len(load_params) > 0:
        return None
    
    datasets = _DATASETS.copy()
    for ds in datasets.values():
        if str(ds.path) == data_src:
            return ds
    
    return None

            
def clear_datasets():
    """
    Removes all saved datasets
    Parameters:
    Output:
    """
    print("\nClearing all saved datasets...")
    _DATASETS = {}


def remove_dataset(src_name):
    """
    Removes the specified dataset from active datasets
    Parameters:
        src_name: Name of dataset to remove
    Output:
    """
    try:
        print('Removing {}'.format(src_name))
        del _DATASETS[src_name]
    except NameError:
        print('ERROR: Could not find dataset {}'.format(src_name))


def _get_compare_df(comp: Comparison, col1_checks: dict, col2_checks: dict, add_diff_col):
    col1 = comp.col1
    col2 = comp.col2
    col1_values = list(col1.get_summary().values()) + list(col1_checks.values())
    col2_values = list(col2.get_summary().values()) + list(col2_checks.values())
    col_keys = list(col1.get_summary().keys()) + list(col1_checks.keys())

    assert len(col1_values) == len(col2_values), \
        '{} values found in {}, but {} found in {}'.format(
            len(col1_values), col1.name, len(col2_values), col2.name
        )

    if comp.col1.name == comp.col2.name:
        col1_name = comp.col1.name + '1'
        col2_name = comp.col2.name + '2'
    else:
        col1_name = comp.col1.name
        col2_name = comp.col2.name

    if add_diff_col:
        checks_added = (len(col1_checks) == len(col2_checks)) and (len(col1_checks) > 0) 
        data = {
            col1_name: col1_values,
            col2_name: col2_values,
            'diff_col': comp.create_diff_column(checks_added=checks_added)
        }
    else:
        data = {
            col1_name: col1_values,
            col2_name: col2_values
        }
        
    _df = pd.DataFrame(
        data,
        index=col_keys
    )
    comp.set_dataframe(_df)
    
    return _df
        
        
def compare(
        ds_pair1: tuple,
        ds_pair2: tuple,
        ds_names: list=None,
        ds_params_list: list=None,
        perform_check: bool=False,
        save_comp: bool=True,
        add_diff_col: bool=False
    ):
    """
    A function for comparing two raw data sources
    Parameters:
        ds_pair1: Tuple with first data source and \
            desired column e.g. ('stocks.parquet', 'price')
        ds_pair2: Tuple with second data source and \
            desired column e.g. ('stocks.parquet', 'price')
        ds_names: List with custom names for ds_pairs. Must provide two names.
        ds_params_list: List with load params for each dataset.
        perform_check: Set as True to perform check for the columns
        compare: Set as True to perform the comparison
        save_comp: Set as True to save the comparison in a \
            global variable
        add_diff_col: Set as True to add a column showing the \
            different between the two columns
    Output:
        Dataframe of compared variables
    """
    assert ds_pair1 and isinstance(ds_pair1, tuple) and len(ds_pair1) == 2, \
        'First dataset and column pair must be provided as a tuple: e.g. (dataset, col)'
    assert ds_pair2 and isinstance(ds_pair2, tuple) and len(ds_pair2) == 2, \
        'Second dataset and column pair must be provided as a tuple: e.g. (dataset, col)'
 
    # need to first process raw data sources into dataset objects
    data_src1 = ds_pair1[0]
    data_src2 = ds_pair2[0]
    ds1, ds2 = load_datasets(
        data_src1, 
        data_src2,
        data_source_names=ds_names,
        load_params_list=[{}, {}]
    )
        
    col_name1 = ds_pair1[1]
    col_name2 = ds_pair2[1]
    
    assert col_name1 in ds1.columns, \
        '{} is not a valid column in dataset {}'.format(col_name1, ds1)
    assert col_name2 in ds2.columns, \
        '{} is not a valid column in dataset {}'.format(col_name2, ds2)
        
    col1 = ds1.columns[col_name1]
    col2 = ds2.columns[col_name2]
    col1_checks = {}
    col2_checks = {}
    
    if perform_check:
        col1_checks = col1.perform_check()
        col2_checks = col2.perform_check()
    
    _comp = Comparison(col1, col2)
    _df = _get_compare_df(_comp, col1_checks, col2_checks, add_diff_col)
    
    if save_comp:
        _COMPARISONS[_comp.name] = _comp
    
    return _df


def compare_ds(
        col1: Column,
        col2: Column,
        perform_check: bool=False,
        save_comp: bool=True,
        add_diff_col: bool=False
    ):
    """
    A function for comparing two dataset objects
    Parameters:
        col1: Desired column to compare to
        col2: Desired columns to compare against
        ds_names: List with custom names for ds_pairs. Must provide two names.
        ds_params_list: List with load params for each dataset.
        perform_check: Set as True to perform check for the columns
        compare: Set as True to perform the comparison
        save_comp: Set as True to save the comparison in a \
            global variable
        add_diff_col: Set as True to add a column showing the \
            different between the two columns
    Output:
        Dataframe of compared variables
    """
    assert isinstance(col1, Column), \
        'Column 1 is not a valid column'
    assert isinstance(col2, Column), \
        'Column 2 is not a valid column'
        
    col1_checks = {}
    col2_checks = {}
    
    if perform_check:
        col1_checks = col1.perform_check()
        col2_checks = col2.perform_check()
    
    _comp = Comparison(col1, col2)
    _df = _get_compare_df(_comp, col1_checks, col2_checks, add_diff_col)
    
    if save_comp:
        _COMPARISONS[_comp.name] = _comp
    
    return _df


def get_comparisons():
    return _COMPARISONS


def get_comparison(comp_name):
    return _COMPARISONS[comp_name]


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
        print('Could not find comparison {}'.format(comp_name))


def clear_comparisons():
    """Removes all active copmarisons"""
    print("\nClearing all active comparisons...")
    _COMPARISONS = {}
     

def profile(dataset: Dataset, col_list: list, name: str=None):
    assert isinstance(dataset.__class__, Dataset.__class__), \
        "Data source must be of type 'Dataset'"

    ds_profile = {}
    if '*' in col_list:
        for col in dataset.columns:
            col_full = dataset.name + '.' + col.name
            ds_profile[col_full] = col.get_summary()
            
    for col_name in col_list:
        col = dataset.columns[col_name]
        col_full = dataset.name + '.' + col.name
        ds_profile[col_full] = col.get_summary()

    if str:
        _PROFILE[name] = ds_profile
    else:
        profile_name = 'profile_' + len(_PROFILE)
        _PROFILE[profile_name] = ds_profile
        
    return ds_profile
    
          
def clear_all():
     """Removes all active datasets and copmarisons"""
     clear_datasets()
     clear_comparisons()


def view(comp_name):
    print(_COMPARISONS[comp_name].dataframe)


def main():
    return 0


if __name__ == '__main__':
    main()
    sys.exit(0)
