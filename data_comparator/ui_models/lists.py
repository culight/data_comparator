"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    GUI Models are housed here

### DEVELOPER NOTES:
"""
import logging
import typing

from pandas.core.algorithms import value_counts
import sys
import logging
from pathlib import Path

from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtPrintSupport import *
from PyQt5 import uic

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.figure import Figure
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg,
)

import data_comparator.data_comparator as dc
from .utilities import FileLoader

UI_DIR = Path(__file__).parent.parent / "ui"
DETAIL_DLG_DIR = str(UI_DIR / "data_detail_dialog.ui")
INPUT_PARAMS_DLG_DIR = str(UI_DIR / "input_parameters_dialog.ui")
ACCEPTED_INPUT_FORMATS = ["sas7bdat", "csv", "parquet", "json"]
NON_PLOT_ROWS = ["ds_name", "name", "data_type"]

DATASET1 = None
DATASET2 = None
VALIDS = {}

logging.basicConfig(
    stream=sys.stdout, format="%(asctime)s - %(message)s", level=logging.DEBUG
)
LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class DatasetColumnsListModel(QAbstractListModel, FileLoader):
    def __init__(self, dataset=None, ds_num=None, parent=None):
        super(DatasetColumnsListModel, self).__init__(
            dataset=dataset,
            ds_num=ds_num,
            parent_fileloader=parent
        )
        self.cols = ["====="]
        self.filename = None
        if dataset != None:
            self.cols = self.cols + list(sorted(dataset.columns.keys()))
            self.dataset = dataset
            self.filename = dataset.path
        else:
            self.dataset = None
    
    def reset(self):
        self.cols = ["====="]
        self.filename = None
        self.dataset = None
        
    def canDropMimeData(self, data: 'QMimeData', action: Qt.DropAction, row: int, column: int, parent: QModelIndex) -> bool:
        filename = data.urls()[0].toLocalFile()
        if str(filename) == str(self.filename):
            return super().canDropMimeData(data, action, row, column, parent)

        if filename:
            file_type = Path(filename).name.split(".")[-1]
            if file_type not in ACCEPTED_INPUT_FORMATS:
                return super().canDropMimeData(data, action, row, column, parent)

        self.filename = filename
        self._load_data(fname=self.filename)
        return super().canDropMimeData(data, action, row, column, parent)

    def dropMimeData(self, data: 'QMimeData', action: Qt.DropAction, row: int, column: int, parent: QModelIndex) -> bool:
        return super().dropMimeData(data, action, row, column, parent)

    def mimeTypes(self) -> typing.List[str]:
        return super().mimeTypes()

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        return super().flags(index)

    def mimeTypes(self) -> typing.List[str]:
        return super().mimeTypes()

    def supportedDropActions(self) -> Qt.DropActions:
        return Qt.CopyAction

    def data(self, index, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            col_name = self.cols[index.row()]
            return col_name

    def rowCount(self, parent=QModelIndex()):
        return len(self.cols)