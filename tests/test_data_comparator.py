import pytest
import logging
import re
from pathlib import Path

import data_comparator.data_comparator as dc

VALID_DATA_TYPES = ("json", "avro", "csv", "sas7bdat", "parquet")
TEST_DATA_DIR = "tests/test_data"

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class DataComparatorHarness:
    def __init__(self, ds_types):
        LOGGER.info("initializing data comparator test harness...")
        self.ds_types = ds_types
        self.test_data_paths = {key: [] for key in self.ds_types}
        self.test_data = {key: [] for key in self.ds_types}
        self._load_data_paths()

    def _load_data_paths(self):
        for file_type in Path(TEST_DATA_DIR).glob("*"):
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


@pytest.fixture
def dc_harness(ds_types_fix):
    return DataComparatorHarness(ds_types_fix)


def test_load_dataset_csv(dc_harness):
    LOGGER.info("testing csv loading...")

    ds_type = "csv"
    if ds_type not in dc_harness.ds_types:
        pytest.skip("not testing {}".format(ds_type))

    dc_harness.load_datasets(ds_type=ds_type)

    assert dc_harness, "test harness not created properly"
    assert len(dc_harness.test_data_paths[ds_type]) == len(
        dc_harness.test_data[ds_type]
    ), "not all {} datasets were loaded: \n{} \n{}".format(
        ds_type, dc_harness.test_data_paths[ds_type], dc_harness.test_data[ds_type]
    )

    for dataset in dc_harness.test_data[ds_type]:
        assert (
            dataset != None and len(dataset.dataframe.index) >= 0
        ), "dataset {} is empty".format(dataset.name)


def test_load_dataset_parquet(dc_harness):
    LOGGER.info("testing parquet loading")

    ds_type = "parquet"
    if ds_type not in dc_harness.ds_types:
        pytest.skip("not testing {}".format(ds_type))

    dc_harness.load_datasets(ds_type=ds_type)

    assert dc_harness, "test harness not created properly"
    assert len(dc_harness.test_data_paths[ds_type]) == len(
        dc_harness.test_data[ds_type]
    ), "not all {} datasets were loaded".format(ds_type)

    for dataset in dc_harness.test_data[ds_type]:
        assert (
            dataset != None and len(dataset.dataframe.index) >= 0
        ), "dataset {} is empty".format(dataset.name)


def test_load_dataset_sas(dc_harness):
    LOGGER.info("testing SAS loading")

    ds_type = "sas7bdat"
    if ds_type not in dc_harness.ds_types:
        pytest.skip("not testing {}".format(ds_type))

    dc_harness.load_datasets(ds_type=ds_type)

    assert dc_harness, "test harness not created properly"
    assert len(dc_harness.test_data_paths[ds_type]) == len(
        dc_harness.test_data[ds_type]
    ), "not all {} datasets were loaded".format(ds_type)

    for dataset in dc_harness.test_data[ds_type]:
        assert (
            dataset != None and len(dataset.dataframe.index) >= 0
        ), "dataset {} is empty".format(dataset.name)


def test_load_dataset_json(dc_harness):
    LOGGER.info("testing json loading")

    ds_type = "json"
    if ds_type not in dc_harness.ds_types:
        pytest.skip("not testing {}".format(ds_type))

    dc_harness.load_datasets(ds_type=ds_type)

    assert dc_harness, "test harness not created properly"
    assert len(dc_harness.test_data_paths[ds_type]) == len(
        dc_harness.test_data[ds_type]
    ), "not all {} datasets were loaded".format(ds_type)

    for dataset in dc_harness.test_data[ds_type]:
        assert (
            dataset != None and len(dataset.dataframe.index) >= 0
        ), "dataset {} is empty".format(dataset.name)


# @pytest.fixture
# def dc_loaded_harness(ds_types_fix):
#     dc_harness = DataComparatorHarness(ds_types_fix)
#     return dc_harness.load_all_datasets()


# def test_get_dataset(dc_loaded_harness):
#     """
#     [summary]
#     """
#     pass


# def test_get_datasets(dc_loaded_harness):
#     """
#     [summary]
#     """
#     pass


# def test_pop_dataset():
#     """
#     [summary]
#     """
#     pass


# def test_pop_datasets():
#     """
#     [summary]
#     """
#     pass


# def test_clear_datasets():
#     """
#     [summary]
#     """
#     pass


# def test_remove_dataset():
#     pass


# def test_compare():
#     pass


# def test_compare_ds():
#     pass


# def test_get_comparisons():
#     pass


# def test_get_comparison():
#     pass


# def test_get_comparisons():
#     pass


# def test_pop_comparison():
#     pass


# def test_pop_comparisons():
#     pass


# def test_remove_comparison():
#     pass


# def test_remove_comparisons():
#     pass


# def test_clear_comparison():
#     pass


# def test_profile():
#     pass


# def test_pop_all():
#     pass


# def test_view():
#     pass
