def pytest_addoption(parser):
    parser.addoption(
        "--exten",
        action="store",
        default="json, avro, csv, sas7bdat, parquet",
        help='Add data exten to test separated by comma: e.g. "csv, parquet"',
    )


def pytest_generate_tests(metafunc):
    option_value = metafunc.config.option.exten
    if "exten" in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("exten", [option_value])
