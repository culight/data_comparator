import os
import sys

from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtPrintSupport import *
from PyQt5 import uic

from ui.custom_buttons import *

UI_TEMPLATE = "templates/data_comparator.ui"
# ---------------------------------------------------------------------------------------
#
# ---------------------------------------------------------------------------------------


class MainWindow(QMainWindow):
    def __init__(self, *args, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        uic.loadUi(UI_TEMPLATE, self)

        # wire up the various buttons
        self.create_buttons()
        self.show()

    def create_buttons(self):
        """Load all of the button objects with parent QPushButton"""

        # wire the file load buttons
        self.dataset1FileLoad = SelectFileButton(
            self.findChild(QPushButton, "dataset1FileLoad")
        )
        self.dataset2FileLoad = SelectFileButton(
            self.findChild(QPushButton, "dataset2FileLoad")
        )

        # wire the data add buttons
        self.addOneButton = SelectFileButton(
            self.findChild(QPushButton, "addOneButton")
        )
        self.addAllButton = SelectFileButton(
            self.findChild(QPushButton, "addAllButton")
        )
        self.removeOneButton = SelectFileButton(
            self.findChild(QPushButton, "removeOneButton")
        )
        self.removeAllButton = SelectFileButton(
            self.findChild(QPushButton, "removeAllButton")
        )

        # wire the data detail buttons
        self.dataDetail1Button = DataDetailButton(
            self.findChild(QPushButton, "datasetDetails1Button")
        )
        self.dataDetail2Button = DataDetailButton(
            self.findChild(QPushButton, "datasetDetails2Button")
        )

        # wire the open config button
        self.openConfigButton = OpenConfigButton(
            self.findChild(QPushButton, "openConfigButton")
        )

        # wire the validation button
        self.addValidationButton = ValidationButton(
            self.findChild(QPushButton, "addValidationButton")
        )
        self.removeValidationButton = ValidationButton(
            self.findChild(QPushButton, "removeValidationButton")
        )

        # wire the compare and reset buttons
        self.compareButton = CompareButton(self.findChild(QPushButton, "compareButton"))
        self.resetButton = CompareButton(self.findChild(QPushButton, "resetButton"))

    def clickCompare(self):
        print("compare")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Data Comparator")

    window = MainWindow()
    app.exec_()
