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

UI_TEMPLATE = "ui/data_comparator.ui"
ACCEPTED_INPUT_FORMATS = ["sas7bdat", "csv", "parquet", "json"]

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class DatasetColumnsListModel(QAbstractListModel):
    def __init__(self, cols):
        super(DatasetColumnsListModel, self).__init__()
        self.cols = cols or []

    def data(self, index, role):
        if role == Qt.DisplayRole:
            col_name = self.cols[index.row()]
            return col_name

    def rowCount(self, index):
        return len(self.cols)


class LoadingProgressBar(QProgressBar):
    def __init__(self, progress_bar):
        super(QProgressBar, self).__init__()

        self.progress_bar = progress_bar

    def modify_progress(self):
        print("setting progress bar")


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
        self.main.load_columns(self.dataset, self.ds_num)


class ColumnSelectButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()


class DataDetailButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()


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
        uic.loadUi(UI_TEMPLATE, self)

        self.dataset1_select_file_button = SelectFileButton(
            self, self.dataset1FileLoad, 1
        )
        self.dataset2_select_file_button = SelectFileButton(
            self, self.dataset2FileLoad, 2
        )

        self.setupLogger()

        self.show()

    def setupLogger(self):
        font = QFont("Arial", 5)
        self.loggingBox.setFont(font)

        logHandler = LogStream(self)
        logHandler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        logging.getLogger().addHandler(logHandler)

    def load_columns(self, dataset, ds_num):
        pass


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Data Comparator")

    window = MainWindow()
    app.exec_()
