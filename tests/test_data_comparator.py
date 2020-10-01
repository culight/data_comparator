from pathlib import Path

import data_comparator.data_comparator as dc

TEST_DATA_DIR = "tests/test_data"

VALID_DATA_TYPES = ('json', 'avro', 'csv', 'sas7bdat', 'parquet')
TEST_DATA_PATH = {key: [] for key in VALID_DATA_TYPES}
TEST_DATA = {key: [] for key in VALID_DATA_TYPES}


def test_load_data():
    for file_type in Path(TEST_DATA_DIR).glob('*'):
        if file_type.stem not in VALID_DATA_TYPES:
            continue
        for file_path in Path(file_type).glob('*'):
            if file_path.suffix.replace('.', '').strip() in VALID_DATA_TYPES:
                print('Loading... ', file_path)
                extension = str(file_type.stem)
                if extension in TEST_DATA_PATH:
                    TEST_DATA_PATH[extension].append(file_path)
                else:
                    print(extension, ' is not a valid extension')
    assert(len(TEST_DATA_PATH) > 0)


def load_dataset(ds_type):
    data_source_paths = TEST_DATA_PATH[ds_type]
    for data_source in data_source_paths:
        data_source_name = data_source.stem
        print('\n', 'Testing ', data_source_name, '...')
        TEST_DATA[ds_type] = dc.load_dataset(
            data_source=data_source,
            data_source_name=data_source_name
        )


def test_load_dataset_csv():
    ds_type = 'csv'
    load_dataset(ds_type=ds_type)
    assert(TEST_DATA[ds_type] != None)


def test_load_dataset_paruqet():
    ds_type = 'parquet'
    load_dataset(ds_type=ds_type)
    assert(TEST_DATA[ds_type] != None)


def test_load_dataset_sas():
    ds_type = 'sas7bdat'
    load_dataset(ds_type=ds_type)
    assert(TEST_DATA[ds_type] != None)
