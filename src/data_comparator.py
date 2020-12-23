"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    Main module for the implementation of data comparator tool

### DEVELOPER NOTES:
"""
# pylint: disable=no-member
import sys
import logging
from typing import Type, Union

import pandas as pd

from components.dataset import Dataset, Column
from components.comparison import Comparison
from components.data_cupboard import DataCupboard

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
LOGGER = logging.getLogger(__name__)
DATA_CUPBOARD = DataCupboard()

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def load_dataset(data_source, data_source_name: str = "", **load_params):
    """
    Load a single data source to add to the set of saved datasets
    Parameters:
        data_source: 
            Object in the form of a csv, parquet, or sas path... 
            or spark/pandas dataframe
        data_source_name: 
            Custom name for the resulting dataset. Default will 
            be provided if null
        other input parameters for the given datas source type:
            e.g. usecols=['id', 'username'] for csv data source
    Output:
        Resulting dataset collection
    """
    assert data_source, "Data source not provided"

    src = data_source

    if data_source_name:
        src_name = data_source_name
    else:
        datasets = DATA_CUPBOARD.read_data("dataset")
        dataset_index = len(datasets)
        src_name = "dataset_" + str(dataset_index)

    LOGGER.info(
        "\nCreating dataset '{}' from source:\n '{}'".format(src_name, src))

    dataset = Dataset(data_src=src, name=src_name, **load_params)
    DATA_CUPBOARD.write_data("dataset", src_name, dataset)

    LOGGER.info('Done loading dataset {}'.format(src_name))

    return dataset


def load_datasets(
    *data_sources, data_source_names: list = None, load_params_list: list = None
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
    assert data_sources, "Valid data source must be provided"

    src_names = []
    for i, src in enumerate(data_sources):
        src_name = None
        load_params = None
        dataset = None
        if data_source_names:
            try:
                src_name = data_source_names[i]
            except IndexError:
                print("Number of names must match number of data sources")
        else:
            datasets = DATA_CUPBOARD.read_data("dataset")
            dataset_index = len(datasets)
            src_name = "dataset_" + str(dataset_index)
            src_names.append(src_name)

        if load_params_list:
            try:
                load_params = load_params_list[i]
            except IndexError:
                print("Number of load parameters must match number of data sources")
            dataset = Dataset(data_src=src, name=src_name, **load_params)
        else:
            dataset = Dataset(data_src=src, name=src_name)

        LOGGER.info("Creating dataset '{}'".format(src_name))

        DATA_CUPBOARD.write_data("dataset", src_name, dataset)

        LOGGER.info("Done loading datasets")

    if not data_source_names:
        data_source_names = src_names

    return [
        DATA_CUPBOARD.read_data("dataset", ds_name) for ds_name in data_source_names
    ]


def get_datasets():
    """
    Return all saved datasets
    Parameters:
    Output:
        All saved datasets
    """

    return DATA_CUPBOARD.read_data("dataset")


def get_dataset(ds_name):
    """
    Return a particular dataset
    Parameters: 
        ds_name: dataset name
    Output:
        The specified dataset
    """
    return DATA_CUPBOARD.read_data("dataset", ds_name)


def pop_dataset(ds_name):
    """
    Return and remove a particular dataset
    Parameters: 
        ds_name: dataset name
    Output:
        The specified dataset
    """
    return DATA_CUPBOARD.pop_data("dataset", ds_name)


def pop_datasets():
    """
    Return and remove all datasets
    Parameters:
    Output:
        All datasets
    """
    return DATA_CUPBOARD.pop_data("dataset")


def clear_datasets():
    """
    Removes all saved datasets
    Parameters:
    Output:
    """
    LOGGER.info("Clearing all saved datasets...")
    DATA_CUPBOARD.remove_data("dataset")
    LOGGER.info("Done clearing datasets")


def remove_dataset(src_name):
    """
    Removes the specified dataset from active datasets
    Parameters:
        src_name: Name of dataset to remove
    Output:
    """
    try:
        LOGGER.info("Removing {}".format(src_name))
        DATA_CUPBOARD.remove_data("dataset", src_name)
    except NameError:
        LOGGER.error("Could not find dataset {}".format(src_name))
    LOGGER.info("Done removing dataset {}".format(src_name))


def _get_compare_df(
    comp: Comparison, col1_checks: dict, col2_checks: dict, add_diff_col
):
    """
    Create dataframe from comparison object
    Parameters:
        comp: comparison object
        col1_checks: column 1 checks dictionary
        col2_checks: column 2 checks dictionary
        add_diff_col: difference column flag
    Output:
        comparison dataframe
    """
    col1 = comp.col1
    col2 = comp.col2
    col1_values = list(col1.get_summary().values()) + \
        list(col1_checks.values())
    col2_values = list(col2.get_summary().values()) + \
        list(col2_checks.values())
    col_keys = list(col1.get_summary().keys()) + list(col1_checks.keys())

    assert len(col1_values) == len(
        col2_values
    ), "{} values found in {}, but {} found in {}".format(
        len(col1_values), col1.name, len(col2_values), col2.name
    )

    if comp.col1.name == comp.col2.name:
        col1_name = comp.col1.name + "1"
        col2_name = comp.col2.name + "2"
    else:
        col1_name = comp.col1.name
        col2_name = comp.col2.name

    if add_diff_col:
        checks_added = (len(col1_checks) == len(
            col2_checks)) and (len(col1_checks) > 0)
        data = {
            col1_name: col1_values,
            col2_name: col2_values,
            "diff_col": comp.create_diff_column(checks_added=checks_added),
        }
    else:
        data = {col1_name: col1_values, col2_name: col2_values}

    _df = pd.DataFrame(data, index=col_keys)
    comp.set_dataframe(_df)

    return _df


def compare(
    data_source1,
    data_source2,
    col_pairs: Union[list, tuple] = None,
    ds_name1: str = None,
    ds_name2: str = None,
    perform_check: bool = False,
    save_comp: bool = True,
    add_diff_col: bool = False,
):
    """
    A function for comparing two raw data sources
    Parameters:
        data_source1: Tuple with first data source and \
            desired column e.g. ('stocks.parquet', 'price')
        data_source2: Tuple with second data source and \
            desired column e.g. ('stocks.parquet', 'price')
        col_pairs: tuple with column 1 and column 2 names to compare \
            or a list with such tuples. Must provide two names.
        ds_name1: Name of the first data source
        ds_name2: Name of the second data source
        perform_check: Set as True to perform check for the columns
        save_comp: Set as True to save the comparison in a \
            global variable
        add_diff_col: Set as True to add a column showing the \
            different between the two columns
    Output:
        Dataframe of compared variables
    """
    _df = None
    assert data_source1 and data_source2, "Two datasets must be provided for comparison"

    # need to first process raw data sources into dataset objects
    ds1, ds2 = load_datasets(
        data_source1,
        data_source2,
        data_source_names=[ds_name1, ds_name2] if (
            ds_name1 or ds_name2) else None,
        load_params_list=[{}, {}],
    )

    compare_by_col = False
    cols_to_compare = list()
    if not col_pairs:
        # name the comparison object after the single column
        compare_by_col = True

        # no column pairs provided for comparison...
        # compare columns with like names
        ds1_cols = list(ds1.columns.keys())
        ds2_cols = list(ds2.columns.keys())

        common_cols = list(set(ds1_cols).intersection(ds2_cols))
        cols_to_compare = [(col, col) for col in common_cols]
    else:
        if type(col_pairs) == list and len(col_pairs) > 0:
            cols_to_compare = col_pairs
        elif type(col_pairs) == tuple and len(col_pairs) == 2:
            cols_to_compare = col_pairs
        else:
            LOGGER.warn("Invalid column pairs entry {}".format(col_pairs))

    for pair in cols_to_compare:
        col_name1 = pair[0]
        col_name2 = pair[1]

        assert (
            col_name1 in ds1.columns
        ), "{} is not a valid column in dataset {}".format(col_name1, ds1)
        assert (
            col_name2 in ds2.columns
        ), "{} is not a valid column in dataset {}".format(col_name2, ds2)

        col1 = ds1.columns[col_name1]
        col2 = ds2.columns[col_name2]
        col1_checks = {}
        col2_checks = {}

        if perform_check:
            col1_checks = col1.perform_check()
            col2_checks = col2.perform_check()

        _comp = Comparison(col1, col2, compare_by_col)
        _df = _get_compare_df(_comp, col1_checks, col2_checks, add_diff_col)

        if save_comp:
            DATA_CUPBOARD.write_data("comparison", _comp.name, _comp)

    return _df


def compare_ds(
    col1: Column,
    col2: Column,
    perform_check: bool = False,
    save_comp: bool = True,
    add_diff_col: bool = False,
    compare_by_col: bool = False,
):
    """
    A function for comparing two dataset objects
    Parameters:
        col1: Desired column to compare to
        col2: Desired columns to compare against
        perform_check: Set as True to perform check for the columns
        save_comp: Set as True to save the comparison in a \
            global variable
        add_diff_col: Set as True to add a column showing the \
            different between the two columns
        compare_by_col: Compare columns with the same names
    Output:
        Dataframe of compared variables
    """
    assert isinstance(col1, Column), "Column 1 is not a valid column"
    assert isinstance(col2, Column), "Column 2 is not a valid column"

    col1_checks = {}
    col2_checks = {}

    if perform_check:
        col1_checks = col1.perform_check()
        col2_checks = col2.perform_check()

    _comp = Comparison(col1, col2, compare_by_col)
    _df = _get_compare_df(_comp, col1_checks, col2_checks, add_diff_col)

    if save_comp:
        DATA_CUPBOARD.write_data("comparison", _comp.name, _comp)

    return _df


def get_comparisons():
    """
    Retrieve all comparison objects
    Parameters:
    Ouputs:
        Comparison objects
    """
    return DATA_CUPBOARD.read_data("comparison")


def get_comparison(comp_name):
    """
    Retrieve a particular comparison object
    Parameters:
        comp_name: Name of comparison
    Ouputs:
        Comparison object
    """
    return DATA_CUPBOARD.read_data("comparison", comp_name)


def pop_comparison(comp_name):
    """
    Retrieve and remove a particular comparison object
    Parameters:
        comp_name: Name of comparison
    Ouputs:
        Comparison object
    """
    return DATA_CUPBOARD.pop_data("comparison", comp_name)


def pop_comparisons():
    """
    Retrieve and remove all comparison objects
    Parameters:
    Ouputs:
        Comparison objects
    """
    return DATA_CUPBOARD.pop_data("comparison")


def remove_comparison(comp_name):
    """
    Removes the specified comparison from active datasets
    Parameters:
        comp_name: Name of comparison to remove
    """
    try:
        LOGGER.info("Removing comparison {}".format(comp_name))
        DATA_CUPBOARD.remove_data("comparison", comp_name)
    except NameError:
        LOGGER.warn("Could not find comparison {}".format(comp_name))
    LOGGER.info("Done removing comparison")


def clear_comparisons():
    """Removes all active copmarisons"""
    LOGGER.info("Clearing all active comparisons...")
    DATA_CUPBOARD.remove_data("comparison")
    LOGGER.info("Done clearing comparisons")


def profile(column: Column):
    """
    Performs summary of single column object
    Parameters:
        column: Column object to profile
    Outputs:
        profile dataframe
    """
    col_full = ".".join((column.ds_name, column.name))
    profile_init = pd.DataFrame.from_dict(column.get_summary(), orient="index")
    profile = profile_init.rename(columns={0: col_full})

    DATA_CUPBOARD.write_data("profile", col_full, profile)

    return profile


def pop_all():
    """Retrieve and remove all active copmarisons"""
    return DATA_CUPBOARD.pop_data()


def clear_all():
    """Removes all active datasets and copmarisons"""
    DATA_CUPBOARD.remove_data()


def view(comp_name):
    """
    Performs summary of single column object
    Parameters:
        column: Column object to profile
    Outputs:
        profile dataframe
    """
    comp = DATA_CUPBOARD.read_data("comparison", comp_name)
    print(comp.dataframe)
