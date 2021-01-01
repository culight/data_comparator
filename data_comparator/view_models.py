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

UI_DIR = Path(__file__).parent / "ui"
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

class FileLoader():
    def __init__(self, dataset=None, ds_num=None, parent_fileloader=None):
        self.ds_num = ds_num
        self.parent_fileloader = parent_fileloader
        self.dataset = dataset
    
    def _set_input_params(self):
        self.input_params = {}
        value_subs = {
            'none': None,
            'null': None,
            "true": True,
            "false": False
        }
        settings = QSettings('myorg', 'myapp' + str(self.ds_num))
        param_values = settings.value('params', [])
        if len(param_values) > 0:
            for v in param_values:
                key = v[0].lower().replace(' ', '')
                value = v[1].lower().replace(' ', '')

                if not key:
                    # ignore entries with empty keys
                    continue

                if value in value_subs.keys():
                    value = value_subs[value]
                if (',' in value) and (len(value) > 1):
                    value = value.split(',')

                self.input_params.update({key: value})
    
    def _load_data(self, fname: str):
        if not self.ds_num:
            return

        data_path = Path(fname)
        if (".part-" in fname) or ("._SUCCESS" in fname):
            data_path = data_path.parent_fileloader

        self._set_input_params()

        file_type = data_path.name.split(".")[-1]
        ds_postfix = "_ds" + str(self.ds_num)
        dataset_name = data_path.stem + ds_postfix

        assert (
            file_type in ACCEPTED_INPUT_FORMATS
        ), "Select file type was {}, but must be in format {}".format(
            ",".join([" *." + frmt for frmt in ACCEPTED_INPUT_FORMATS])
        )
        try:
            self.dataset = dc.load_dataset(
                data_source=data_path,
                data_source_name=dataset_name,
                **self.input_params
            )
        except (TypeError, AttributeError, ValueError) as e:
            LOGGER.error(str(e))

        self._onDatasetLoaded()

    def _onDatasetLoaded(self):
        self.parent_fileloader.render_data(self.dataset, self.ds_num)


class SelectFileButton(QPushButton, FileLoader):
    def __init__(self, button, ds_num, parent):
        super(SelectFileButton, self).__init__(
            ds_num=ds_num,
            parent_fileloader=parent
        )
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
            self.detailDialogTable.setItem(
                index, 0, QTableWidgetItem(str(detail_name)))
            self.detailDialogTable.setItem(
                index, 1, QTableWidgetItem(str(detail_val)))
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


class InputParametersDialog(QDialog):
    def __init__(self, num):
        super(InputParametersDialog, self).__init__()
        uic.loadUi(INPUT_PARAMS_DLG_DIR, self)

        # set the initial value
        self.input_params = [['', '']]
        self.num = num
        self.restoreSettings()

        # set up the parameters table
        self.setup_table()

        # set input parameter buttons
        self.add_one_button = AddInputParamButton(self.addParamButton, self)
        self.add_all_button = RemoveInputParamButton(
            self.removeParamButton, self)

    def setup_table(self):
        self.inputParamsTableModel = InputParamsTableModel(self.input_params)
        self.inputParametersTable.setModel(self.inputParamsTableModel)
        nameLineEdit = LineEditDelegate(self, 'name')
        nameLineEdit.cellEditingStarted.connect(self.getUpdatedData)
        self.inputParametersTable.setItemDelegateForColumn(
            0, nameLineEdit)
        valueLineEdit = LineEditDelegate(self, 'value')
        valueLineEdit.cellEditingStarted.connect(self.getUpdatedData)
        self.inputParametersTable.setItemDelegateForColumn(
            1, valueLineEdit)
        self.inputParametersTable.resizeColumnToContents(1)
        self.inputParametersTable.horizontalHeader().setStretchLastSection(True)

    def add_input_parameter(self):
        # don't create new rows until values are added
        self.input_params.append(
            ['', ''])
        self.inputParametersTable.model().layoutChanged.emit()

    def remove_input_parameter(self):
        if not self.inputParametersTable.selectionModel().hasSelection():
            LOGGER.error("Must select a row/rows to remove")
            return

        comp_indices = self.inputParametersTable.selectionModel().selectedRows()

        for index in sorted(comp_indices):
            if len(self.input_params) == 1:
                self.input_params = [['', '']]
                self.setup_table()
                return
            else:
                del self.input_params[index.row()]
                self.inputParametersTable.model().layoutChanged.emit()

    def getUpdatedData(self, row, col, value):
        self.input_params[row][col] = value
        self.saveSettings()

    def saveSettings(self):
        settings = QSettings('myorg', 'myapp' + str(self.num))
        settings.setValue('params', self.input_params)

    def restoreSettings(self):
        settings = QSettings('myorg', 'myapp' + str(self.num))
        self.input_params = settings.value('params', self.input_params)

    def closeEvent(self, event):
        self.saveSettings()
        super(InputParametersDialog, self).closeEvent(event)


class InputParametersButton(QPushButton):
    def __init__(self, button, num):
        super(QPushButton, self).__init__()
        self.num = num
        self.button = button
        self.button.clicked.connect(self.onClicked)

    def onClicked(self):
        DETAIL_DLG_DIR = InputParametersDialog(self.num)
        DETAIL_DLG_DIR.exec_()


class OpenConfigButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()


class ValidationButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()


class CompareButton(QPushButton):
    def __init__(self, button, parent=None):
        super(QPushButton, self).__init__()
        self.button = button
        self.parent = parent
        self.button.clicked.connect(self.compare)


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


class ResetButton(QPushButton):
    def __init__(self, button):
        super(QPushButton, self).__init__()


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
                if data in ["same", "NaT"]:
                    return QBrush(Qt.green)
                else:
                    return QBrush(Qt.red)

        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.df.columns[col]
        if orientation == Qt.Vertical and role == Qt.DisplayRole:
            return self.vertical_header[col]
        return None

    def clear(self):
        self.df = self.df.iloc[0:0]


class LineEditDelegate(QItemDelegate):
    cellEditingStarted = pyqtSignal(int, int, str)

    def __init__(self, parent, setting=None):
        QItemDelegate.__init__(self, parent)
        self.setting = setting

    def _is_valid(self, value):
        # for config table
        if self.setting == 'value':
            try:
                float(value)
            except ValueError:
                LOGGER.error("Value must be numeric")
                return False
            return True
        elif self.setting == 'field':
            try:
                value.split(",")
            except AttributeError:
                LOGGER.error(
                    "Must provide fields in the follwing form: field1, field2, ...")
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


class ComboBoxDelegate(QItemDelegate):
    def __init__(self, parent):
        QItemDelegate.__init__(self, parent)
        self.choices = ['True', 'False']

    def createEditor(self, parent, option, index):
        combobox = QComboBox(parent)
        combobox.addItems(self.choices)
        return combobox

    def setEditorData(self, editor, index):
        value = index.data(Qt.DisplayRole)
        num = self.choices.index(value)
        editor.setCurrentIndex(num)

    def setModelData(self, editor, model, index):
        value = editor.currentText()
        value_pair = (value, 'enabled')
        model.setData(index, value_pair, Qt.DisplayRole)


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
            self.cols = self.cols + list(dataset.columns.keys())
            self.dataset = dataset
            self.filename = dataset.path
        else:
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


class ComparisonsComboBox(QComboBox):
    def __init__(self, comparisons, parent=None):
        super(ComparisonsComboBox, self).__init__(parent)
        self.comparisons = comparisons


class Plot(FigureCanvasQTAgg):
    def __init__(self, parent=None):
        fig = Figure(figsize=(0.5, 0.5), dpi=42)
        fig.clear()
        self.ax = fig.add_subplot(111)
        FigureCanvasQTAgg.__init__(self, fig)


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
