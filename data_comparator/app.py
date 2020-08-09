import PyQt5
from components.dataset import Dataset
import os
import sys
import logging
from pathlib import Path

import data_comparator as dc

from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtPrintSupport import *
from PyQt5 import uic

MAIN_UI = "ui/data_comparator.ui"
DETAIL_DLG = "ui/data_detail_dialog.ui"
ACCEPTED_INPUT_FORMATS = ["sas7bdat", "csv", "parquet", "json"]

DATASET1 = None
DATASET2 = None

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class DatasetColumnsList(QListWidget):
    def __init__(self, cols):
        super(DatasetColumnsListModel, self).__init__()
        self.cols = cols or []

    def data(self, index, role):
        if role == Qt.DisplayRole:
            col_name = self.cols[index.row()]
            return col_name

    def rowCount(self, index):
        return len(self.cols)


class SelectFileButton(QPushButton):
    def __init__(self, main, button, ds_num):
        super(SelectFileButton, self).__init__()
        self.main = main
        self.ds_num = ds_num
        self.btn = button
        self.btn.clicked.connect(self.getFile)
        self.dataset = None

    def getFile(self):
        print("clicked")
        file_diag = QFileDialog()
        fname = file_diag.getOpenFileName(
            self,
            "Open file",
            "c:\\",
            "Data Files ({}, *)".format(
                ",".join(["*." + frmt for frmt in ACCEPTED_INPUT_FORMATS])
            ),
        )[0]

        if not fname:
            return

        data_path = Path(fname)

        if (".part-" in fname) or ("._SUCCESS" in fname):
            data_path = data_path.parent

        file_type = data_path.name.split(".")[-1]
        ds_postfix = "_ds" + str(self.ds_num)
        dataset_name = data_path.stem + ds_postfix

        assert (
            file_type in ACCEPTED_INPUT_FORMATS
        ), "Select file type was {}, but must be in format {}".format(
            ",".join([" *." + frmt for frmt in ACCEPTED_INPUT_FORMATS])
        )

        self.dataset = dc.load_dataset(data_path, dataset_name)

        self.onDatasetLoaded()

    def onDatasetLoaded(self):
        self.main.render(self.dataset, self.ds_num)


class ColumnSelectButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()


class DataDetailDialog(QDialog):
    def __init__(self, dataset):
        super(DataDetailDialog, self).__init__()
        uic.loadUi(DETAIL_DLG, self)

        entries = dataset.get_summary()
        entries.pop("columns")
        entries = self.get_coltypes(dataset, entries)
        self.detailDialogTable.setRowCount(len(entries))
        self.detailDialogTable.setColumnCount(2)
        for index, (detail_name, detail_val) in enumerate(entries.items()):
            self.detailDialogTable.setItem(index, 0, QTableWidgetItem(str(detail_name)))
            self.detailDialogTable.setItem(index, 1, QTableWidgetItem(str(detail_val)))
        self.detailDialogTable.move(0, 0)

    def get_coltypes(self, dataset, entries):
        col_type_template = {
            "string_columns": ["object", "str", "o"],
            "numeric_columns": ["number", "n", "int"],
            "time_columns": ["time", "datetime", "date", "t"],
            "boolean_columns": ["bool", "b"],
        }
        for col_type, type_names in col_type_template.items():
            for type_name in type_names:
                columns = dataset.get_cols_oftype(type_name).values()
                if len(columns) == 0:
                    continue
                ds_names = [col.name for col in columns]
                if col_type in entries:
                    entries[col_type].append(ds_names)
                else:
                    entries[col_type] = ds_names

        return entries


class DatasetDetailsButton(QPushButton):
    def __init__(self, button, dataset=None):
        super(QPushButton, self).__init__()
        self.dataset = dataset
        self.button = button
        self.button.clicked.connect(self.onClicked)

    def onClicked(self):
        if self.dataset != None:
            detail_dlg = DataDetailDialog(self.dataset)
            detail_dlg.exec_()


class OpenConfigButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()


class ValidationButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()


class CompareButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()


class ResetButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()


class DataframeTableModel(QAbstractTableModel):
    def __init__(self, df):
        QAbstractTableModel.__init__(self)
        self.df = df

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


class LogStream(logging.StreamHandler):
    def __init__(self, parent=None):
        super().__init__()
        self.logging_box = parent.loggingBox
        self.setStream(sys.stdout)

    def emit(self, text):
        message = self.format(text)
        self.logging_box.appendPlainText(str(message))

    def write(self, text):
        pass

    def flush(self):
        pass


class MainWindow(QMainWindow):
    def __init__(self, *args, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        uic.loadUi(MAIN_UI, self)

        self.setup_logger()

        self.dataset1_select_file_button = SelectFileButton(
            self, self.dataset1FileLoad, 1
        )
        self.dataset2_select_file_button = SelectFileButton(
            self, self.dataset2FileLoad, 2
        )

        self.show()

    def setup_logger(self):
        font = QFont("Arial", 5)
        self.loggingBox.setFont(font)

        logHandler = LogStream(self)
        logHandler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        logging.getLogger().addHandler(logHandler)

    def render(self, dataset, ds_num):
        if ds_num == 1:
            DATASET1 = dataset

            # set dataframe table
            df_model = DataframeTableModel(DATASET1.dataframe)
            self.dataframe1Table.setModel(df_model)
            self.ds_details_button1 = DatasetDetailsButton(
                self.datasetDetails1Button, dataset
            )
        if ds_num == 2:
            DATASET2 = dataset

            # set dataframe table
            df_model = DataframeTableModel(DATASET2.dataframe)
            self.dataframe2Table.setModel(df_model)
            self.ds_details_button2 = DatasetDetailsButton(
                self.datasetDetails2Button, dataset
            )


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Data Comparator")

    window = MainWindow()
    app.exec_()
