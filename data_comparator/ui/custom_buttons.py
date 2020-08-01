import sys
from pathlib import Path

from PyQt5.QtCore import *
from PyQt5.QtWidgets import *

sys.path.insert(1, r"H:\repos\data_comparator\data_comparator")
import data_comparator as dc

ACCEPTED_INPUT_FORMATS = ["sas7bdat", "csv", "parquet", "json"]

# ---------------------------------------------------------------------------------------
#
# ---------------------------------------------------------------------------------------


class SelectFileButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()

        self.btn = button
        self.btn.clicked.connect(self.getfile)

    def getfile(self):
        file_diag = QFileDialog()
        fname = file_diag.getOpenFileName(
            self,
            "Open file",
            "c:\\",
            "Data Files ({}, *)".format(
                ",".join(["*." + frmt for frmt in ACCEPTED_INPUT_FORMATS])
            ),
        )[0]
        data_path = Path(fname)

        if ".part-" in fname:
            data_path = data_path.parent

        file_type = data_path.name.split(".")[-1]
        dataset_name = data_path.stem
        assert (
            file_type in ACCEPTED_INPUT_FORMATS
        ), "Select file type was {}, but must be in format {}".format(
            ",".join([" *." + frmt for frmt in ACCEPTED_INPUT_FORMATS])
        )

        print(data_path, dataset_name)
        dataset = dc.load_dataset(data_path, dataset_name)

        print(dataset)


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
