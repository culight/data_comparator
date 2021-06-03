"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    GUI Models are housed here

### DEVELOPER NOTES:
"""
import sys
import logging
from pathlib import Path

from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtPrintSupport import *
from PyQt5 import uic

from .utilities import FileLoader
from .tables import InputParamsTableModel

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


class SelectFileButton(QPushButton, FileLoader):
    def __init__(self, button, ds_num, parent):
        super(SelectFileButton, self).__init__(ds_num=ds_num, parent_fileloader=parent)
        self.btn = button
        self.btn.clicked.connect(self.getFile)
        self.dataset = None

    def getFile(self):
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

        self._load_data(fname=fname)


class ColumnSelectButton(QPushButton):
    def __init__(self, button, mode, parent=None):
        super(QPushButton, self).__init__()
        self.button = button
        self.mode = mode
        self.button.clicked.connect(self.onClicked)
        self.parent = parent

    def onClicked(self):
        if self.mode == "add_one":
            self.parent.add_comparison()
        if self.mode == "add_all":
            self.parent.add_comparisons()
        if self.mode == "remove_one":
            self.parent.remove_comparison()
        if self.mode == "remove_all":
            self.parent.clear_comparisons()


class DataDetailDialog(QDialog):
    def __init__(self, dataset):
        super(DataDetailDialog, self).__init__()
        uic.loadUi(DETAIL_DLG_DIR, self)

        self.detailDialogTable.horizontalHeader().setSectionResizeMode(
            QHeaderView.Interactive
        )

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
            DETAIL_DLG_DIR = DataDetailDialog(self.dataset)
            DETAIL_DLG_DIR.exec_()


class InputParametersButton(QPushButton):
    def __init__(self, button, num):
        super(QPushButton, self).__init__()
        self.num = num
        self.button = button
        self.button.clicked.connect(self.onClicked)

    def onClicked(self):
        DETAIL_DLG_DIR = InputParametersDialog(self.num)
        DETAIL_DLG_DIR.exec_()


class ValidationButton(QPushButton):
    def __init__(self, button, parent=None):
        super(QPushButton, self).__init__()
        self.button = button
        self.parent = parent
        self.button.clicked.connect(self.parent.export_validations)

class CompareButton(QPushButton):
    def __init__(self, button, parent=None):
        super(QPushButton, self).__init__()
        self.button = button
        self.parent = parent
        self.button.clicked.connect(self.parent.compare)


class AddInputParamButton(QPushButton):
    def __init__(self, button, parent=None):
        super(QPushButton, self).__init__()
        self.button = button
        self.parent = parent
        self.button.clicked.connect(self.parent.add_input_parameter)


class RemoveInputParamButton(QPushButton):
    def __init__(self, button, parent=None):
        super(QPushButton, self).__init__()
        self.button = button
        self.parent = parent
        self.button.clicked.connect(self.parent.remove_input_parameter)


class ExportButton(QPushButton):
    def __init__(self, button, parent=None):
        super(QPushButton, self).__init__()
        self.button = button
        self.parent = parent
        self.button.clicked.connect(self.parent.export_html_report)


class ResetButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()


# dialogs
class DataDetailDialog(QDialog):
    def __init__(self, dataset):
        super(DataDetailDialog, self).__init__()
        uic.loadUi(DETAIL_DLG_DIR, self)

        self.detailDialogTable.horizontalHeader().setSectionResizeMode(
            QHeaderView.Interactive
        )

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


class InputParametersDialog(QDialog):
    def __init__(self, num):
        super(InputParametersDialog, self).__init__()
        uic.loadUi(INPUT_PARAMS_DLG_DIR, self)

        # set the initial value
        self.input_params = [["", ""]]
        self.num = num
        self.restoreSettings()

        # set up the parameters table
        self.setup_table()

        # set input parameter buttons
        self.add_one_button = AddInputParamButton(self.addParamButton, self)
        self.add_all_button = RemoveInputParamButton(self.removeParamButton, self)

    def setup_table(self):
        self.inputParamsTableModel = InputParamsTableModel(self.input_params)
        self.inputParametersTable.setModel(self.inputParamsTableModel)
        nameLineEdit = LineEditDelegate(self, "name")
        nameLineEdit.cellEditingStarted.connect(self.getUpdatedData)
        self.inputParametersTable.setItemDelegateForColumn(0, nameLineEdit)
        valueLineEdit = LineEditDelegate(self, "value")
        valueLineEdit.cellEditingStarted.connect(self.getUpdatedData)
        self.inputParametersTable.setItemDelegateForColumn(1, valueLineEdit)
        self.inputParametersTable.resizeColumnToContents(1)
        self.inputParametersTable.horizontalHeader().setStretchLastSection(True)

    def add_input_parameter(self):
        # don't create new rows until values are added
        self.input_params.append(["", ""])
        self.inputParametersTable.model().layoutChanged.emit()

    def remove_input_parameter(self):
        if not self.inputParametersTable.selectionModel().hasSelection():
            LOGGER.error("Must select a row/rows to remove")
            return

        comp_indices = self.inputParametersTable.selectionModel().selectedRows()

        for index in sorted(comp_indices):
            if len(self.input_params) == 1:
                self.input_params = [["", ""]]
                self.setup_table()
                return
            else:
                del self.input_params[index.row()]
                self.inputParametersTable.model().layoutChanged.emit()

    def getUpdatedData(self, row, col, value):
        self.input_params[row][col] = value
        self.saveSettings()

    def saveSettings(self):
        settings = QSettings("myorg", "myapp" + str(self.num))
        settings.setValue("params", self.input_params)

    def restoreSettings(self):
        settings = QSettings("myorg", "myapp" + str(self.num))
        self.input_params = settings.value("params", self.input_params)

    def closeEvent(self, event):
        self.saveSettings()
        super(InputParametersDialog, self).closeEvent(event)


class LineEditDelegate(QItemDelegate):
    cellEditingStarted = pyqtSignal(int, int, str)

    def __init__(self, parent, setting=None):
        QItemDelegate.__init__(self, parent)
        self.setting = setting

    def _is_valid(self, value):
        # for config table
        if self.setting == "value":
            try:
                float(value)
            except ValueError:
                LOGGER.error("Value must be numeric")
                return False
            return True
        elif self.setting == "field":
            try:
                value.split(",")
            except AttributeError:
                LOGGER.error(
                    "Must provide fields in the following form: field1, field2, ..."
                )
                return False
            return True

    def createEditor(self, parent, option, index):
        lineedit = QLineEdit(parent)
        return lineedit

    def setModelData(self, editor, model, index):
        value = editor.text()
        if value:
            value_pair = (value, self.setting)
            self.cellEditingStarted.emit(index.row(), index.column(), value)
            model.setData(index, value_pair, Qt.DisplayRole)
