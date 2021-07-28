import pytest
import logging
import re
from pathlib import Path

import data_comparator.data_comparator as dc

VALID_DATA_TYPES = ("json", "avro", "csv", "sas7bdat", "parquet")
TEST_DATA_DIR_FUNC = "tests/test_data/functional"
TEST_DATA_DIR_UNIT = "tests/test_data/unit"
TEST_DATA_DIR_INT = "tests/test_data/integration"

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class DataComparatorHarness:
    def __init__(self, ds_types, test_data_dir):
        LOGGER.info("initializing data comparator test harness...")
        self.ds_types = ds_types
        self.test_data_paths = {key: [] for key in self.ds_types}
        self.test_data = {key: [] for key in self.ds_types}
        self.test_data_dir = test_data_dir
        self.load_data_paths()

    def load_data_paths(self):
        for file_type in Path(self.test_data_dir).glob("*"):
            if file_type.stem not in self.ds_types:
                continue
            for file_path in Path(file_type).glob("*"):
                if file_path.suffix.replace(".", "").strip() in self.ds_types:
                    extension = str(file_type.stem)
                    if extension in self.test_data_paths:
                        self.test_data_paths[extension].append(file_path)
                    else:
                        print(extension, " is not a valid extension")

    def load_datasets(self, ds_type=None):
        """
        Test the load_dataset functionality

        Args:
            ds_type ([str]): [file type of the dataset]
        """
        if not ds_type:
            return
        data_source_paths = self.test_data_paths[ds_type]
        for data_source in data_source_paths:
            data_source_name = data_source.stem
            print("Loading ", data_source, "...")
            self.test_data[ds_type].append(
                dc.load_dataset(
                    data_source=data_source, data_source_name=data_source_name
                )
            )

    def load_all_datasets(self):
        for ds_type in self.ds_types:
            data_source_paths = self.test_data_paths[ds_type]
            for data_source in data_source_paths:
                data_source_name = data_source.stem
                print("Loading ", data_source, "...")
                self.test_data[ds_type].append(
                    dc.load_dataset(
                        data_source=data_source, data_source_name=data_source_name
                    )
                )


@pytest.fixture
def ds_types_fix(pytestconfig):
    exten = pytestconfig.getoption("exten")
    ds_types = [
        type.strip()
        for type in re.split(", | ", exten)
        if type.strip() in VALID_DATA_TYPES
    ]
    return ds_types if ds_types else VALID_DATA_TYPES


## FUNCTIONAL TESTS ##


@pytest.mark.functional
@pytest.fixture
def dc_harness_func(ds_types_fix):
    return DataComparatorHarness(
        ds_types=ds_types_fix, test_data_dir=TEST_DATA_DIR_FUNC
    )


@pytest.mark.functional
def test_load_dataset_csv(dc_harness_func):
    LOGGER.info("testing csv loading...")

    ds_type = "csv"
    if ds_type not in dc_harness_func.ds_types:
        pytest.skip("not testing {}".format(ds_type))

    LOGGER.info("outfitting the test harness...")
    dc_harness_func.load_datasets(ds_type=ds_type)

    assert dc_harness_func, "test harness not created properly"
    assert len(dc_harness_func.test_data_paths[ds_type]) == len(
        dc_harness_func.test_data[ds_type]
    ), "not all {} datasets were loaded: \n{} \n{}".format(
        ds_type,
        dc_harness_func.test_data_paths[ds_type],
        dc_harness_func.test_data[ds_type],
    )

    for dataset in dc_harness_func.test_data[ds_type]:
        assert (
            dataset != None and len(dataset.dataframe.index) >= 0
        ), "dataset {} is empty".format(dataset.name)


@pytest.mark.functional
def test_load_dataset_parquet(dc_harness_func):
    LOGGER.info("testing parquet loading")

    ds_type = "parquet"
    if ds_type not in dc_harness_func.ds_types:
        pytest.skip("not testing {}".format(ds_type))

    LOGGER.info("outfitting the test harness...")
    dc_harness_func.load_datasets(ds_type=ds_type)

    assert dc_harness_func, "test harness not created properly"
    assert len(dc_harness_func.test_data_paths[ds_type]) == len(
        dc_harness_func.test_data[ds_type]
    ), "not all {} datasets were loaded".format(ds_type)

    for dataset in dc_harness_func.test_data[ds_type]:
        assert (
            dataset != None and len(dataset.dataframe.index) >= 0
        ), "dataset {} is empty".format(dataset.name)


@pytest.mark.functional
def test_load_dataset_sas(dc_harness_func):
    LOGGER.info("testing SAS loading")

    ds_type = "sas7bdat"
    if ds_type not in dc_harness_func.ds_types:
        pytest.skip("not testing {}".format(ds_type))

    LOGGER.info("outfitting the test harness...")
    dc_harness_func.load_datasets(ds_type=ds_type)

    assert dc_harness_func, "test harness not created properly"
    assert len(dc_harness_func.test_data_paths[ds_type]) == len(
        dc_harness_func.test_data[ds_type]
    ), "not all {} datasets were loaded".format(ds_type)

    for dataset in dc_harness_func.test_data[ds_type]:
        assert (
            dataset != None and len(dataset.dataframe.index) >= 0
        ), "dataset {} is empty".format(dataset.name)


@pytest.mark.functional
def test_load_dataset_json(dc_harness_func):
    LOGGER.info("testing json loading")

    ds_type = "json"
    if ds_type not in dc_harness_func.ds_types:
        pytest.skip("not testing {}".format(ds_type))

    LOGGER.info("outfitting the test harness...")
    dc_harness_func.load_datasets(ds_type=ds_type)

    assert dc_harness_func, "test harness not created properly"
    assert len(dc_harness_func.test_data_paths[ds_type]) == len(
        dc_harness_func.test_data[ds_type]
    ), "not all {} datasets were loaded".format(ds_type)

    for dataset in dc_harness_func.test_data[ds_type]:
        assert (
            dataset != None and len(dataset.dataframe.index) >= 0
        ), "dataset {} is empty".format(dataset.name)


def _column_test(col_name, col):
    try:
        print("Performing check on {} ...".format(col_name))
        col.perform_check()
    except:
        print("perform check failed on {}".format(col_name))
        raise AssertionError


def _dataset_test(ds):
    # columns
    assert len(ds.columns) > 1, "No columns found for dataset {}".format(ds.name)
    for col_name, col in ds.columns.items():
        _column_test(col_name, col)


@pytest.mark.functional
def test_dataset(dc_harness_func):
    """
    [summary]
    """

    LOGGER.info("outfitting the test harness...")
    dc_harness_func.load_all_datasets()

    for ds_type in dc_harness_func.ds_types:
        ds_list = dc_harness_func.test_data[ds_type]
        for ds in ds_list:
            _dataset_test(ds)


## UNIT TESTS ##


@pytest.mark.unit
@pytest.fixture
def dc_harness_unit(ds_types_fix):
    return DataComparatorHarness(
        ds_types=ds_types_fix, test_data_dir=TEST_DATA_DIR_UNIT
    )


@pytest.mark.unit
def test_get_datasets(dc_loaded_harness):
    """
    [summary]
    """

    LOGGER.info("outfitting the test harness...")
    dc_harness_func.load_data_paths(test_data_dir=TEST_DATA_DIR_UNIT)
    dc_harness_func.load_all_datasets()


@pytest.mark.unit
def test_pop_dataset():
    """
    [summary]
    """

    LOGGER.info("outfitting the test harness...")
    dc_harness_func.load_data_paths(test_data_dir=TEST_DATA_DIR_UNIT)
    dc_harness_func.load_all_datasets()


@pytest.mark.unit
def test_pop_datasets():
    """
    [summary]
    """

    LOGGER.info("outfitting the test harness...")
    dc_harness_func.load_data_paths(test_data_dir=TEST_DATA_DIR_UNIT)
    dc_harness_func.load_all_datasets()


@pytest.mark.unit
def test_clear_datasets():
    """
    [summary]
    """

    LOGGER.info("outfitting the test harness...")
    dc_harness_func.load_data_paths(test_data_dir=TEST_DATA_DIR_UNIT)
    dc_harness_func.load_all_datasets()


@pytest.mark.unit
def test_remove_dataset():
    pass


@pytest.mark.unit
def test_compare():
    pass


@pytest.mark.unit
def test_compare_ds():
    pass


@pytest.mark.unit
def test_get_comparisons():
    pass


@pytest.mark.unit
def test_get_comparison():
    pass


@pytest.mark.unit
def test_get_comparisons():
    pass


@pytest.mark.unit
def test_pop_comparison():
    pass


@pytest.mark.unit
def test_pop_comparisons():
    pass


@pytest.mark.unit
def test_remove_comparison():
    pass


@pytest.mark.unit
def test_remove_comparisons():
    pass


@pytest.mark.unit
def test_clear_comparison():
    pass


@pytest.mark.unit
def test_profile():
    pass


@pytest.mark.unit
def test_pop_all():
    pass


@pytest.mark.unit
def test_view():
    pass


## INTEGRATION TESTS ##


@pytest.mark.integration
@pytest.fixture
def dc_harness_int(ds_types_fix):
    return DataComparatorHarness(ds_types=ds_types_fix, test_data_dir=TEST_DATA_DIR_INT)


@pytest.mark.integration
def test_dataset(dc_harness_int):
    """
    [summary]
    """

    LOGGER.info("outfitting the test harness...")
    dc_harness_int.load_all_datasets()

    for ds_type in dc_harness_int.ds_types:
        ds_list = dc_harness_int.test_data[ds_type]
        for ds in ds_list:
            print(ds.name)

