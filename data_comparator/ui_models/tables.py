"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    GUI Models are housed here

### DEVELOPER NOTES:
"""
import sys
import logging
from pathlib import Path

import pandas as pd

from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtPrintSupport import *

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


class InputParamsTableModel(QAbstractTableModel):
    def __init__(self, data=None):
        QAbstractTableModel.__init__(self)
        self.header = ["Name", "Value"]
        self.data = [] if not data else data

    def flags(self, index):
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable

    def rowCount(self, parent=QModelIndex()):
        return len(self.data)

    def columnCount(self, parent=QModelIndex()):
        return 2

    def data(self, index, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            return self.data[index.row()][index.column()]
        return None

    def setData(self, index, value, role):
        if role == Qt.EditRole or role == Qt.DisplayRole:
            self.data[index.row()][index.column()] = value[0]
        return True

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.header[col]
        return None


class ConfigTableModel(QAbstractTableModel):
    def __init__(self, data):
        QAbstractTableModel.__init__(self)
        self.header = ["Name", "Type", "Enabled", "Value", "Fields"]
        self.data = data

    def flags(self, index):
        if index.column() in [2, 3, 4]:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable
        else:
            return Qt.ItemIsEnabled

    def rowCount(self, parent=QModelIndex()):
        return len(self.data)

    def columnCount(self, parent=QModelIndex()):
        return 5

    def data(self, index, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            return list(self.data[index.row()].values())[index.column()]
        return None

    def setData(self, index, value, role):
        if role == Qt.DisplayRole:
            self.data[index.row()][value[1]] = value[0]

        return True

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.header[col]
        return None


class ComparisonOutputTableModel(QAbstractTableModel):
    def __init__(self, df):
        QAbstractTableModel.__init__(self)
        self.df = df
        self.vertical_header = list(df.index)

    def rowCount(self, parent=None):
        return self.df.shape[0]

    def columnCount(self, parent=None):
        return self.df.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            data = str(self.df.iloc[index.row(), index.column()])
            if role == Qt.DisplayRole:
                return data
            if role == Qt.BackgroundRole and (index.column() == 2):
                if data in ["same", "NaT", "diff"]:
                    return QBrush(Qt.white)
                else:
                    if "-" in str(data):
                        return QBrush(Qt.red)
                    else:
                        return QBrush(Qt.green)
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.df.columns[col]
        if orientation == Qt.Vertical and role == Qt.DisplayRole:
            return self.vertical_header[col]
        return None

    def clear(self):
        self.df = self.df.iloc[0:0]


class ComparisonTableModel(QAbstractTableModel):
    def __init__(self, comparisons):
        QAbstractTableModel.__init__(self)
        self.header = ["Name", "Dataset A", "Dataset B"]
        self.rows = comparisons

    def rowCount(self, parent=None):
        return len(self.rows)

    def columnCount(self, parent=None):
        return 3

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return QVariant(self.rows[index.row()][index.column()])
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return QVariant(self.header[col])
        return None


class DataframeTableModel(QAbstractTableModel):
    def __init__(self, df):
        QAbstractTableModel.__init__(self)
        self.df = df.head(300)

    def rowCount(self, parent=None):
        return self.df.shape[0]

    def columnCount(self, parent=None):
        return self.df.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return str(self.df.iloc[index.row(), index.column()])
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.df.columns[col]
        return None
