"""
### CODE OWNERS: Demerrick Moton
### OBJECTIVE:
    data model for data file object
### DEVELOPER NOTES:
"""
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
import re

import pandas as pd
import matplotlib as plt

from components.comparison import Comparison
from components.dataset import Dataset

ACCEPTED_INPUT_FORMATS = [
    "sas7bdat",
    "csv",
    "parquet",
    "pyspark",
    "pandas",
    "json",
    "txt",
]

logging.basicConfig(
    stream=sys.stdout, format="%(asctime)s - %(message)s", level=logging.DEBUG
)
LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class Plot(object):
    def __init__(self, data, data_type: str, name: str):
        pass

