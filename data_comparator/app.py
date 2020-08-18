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

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg,
    NavigationToolbar2QT as NavigationToolbar,
)
from matplotlib.figure import Figure

MAIN_UI = "ui/data_comparator.ui"
DETAIL_DLG = "ui/data_detail_dialog.ui"
ACCEPTED_INPUT_FORMATS = ["sas7bdat", "csv", "parquet", "json"]
NON_PLOT_ROWS = ["ds_name", "name", "data_type"]

DATASET1 = None
DATASET2 = None

logging.basicConfig(
    stream=sys.stdout, format="%(asctime)s - %(message)s", level=logging.DEBUG
)
LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class SelectFileButton(QPushButton):
    def __init__(self, button, ds_num, parent):
        super(SelectFileButton, self).__init__()
        self.parent = parent
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
        self.parent.render_data(self.dataset, self.ds_num)


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
        uic.loadUi(DETAIL_DLG, self)

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
            detail_dlg = DataDetailDialog(self.dataset)
            detail_dlg.exec_()


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


class DatasetColumnsListModel(QAbstractListModel):
    def __init__(self, dataset=None, parent=None):
        super(DatasetColumnsListModel, self).__init__(parent)
        self.cols = []
        if dataset != None:
            self.cols = list(dataset.columns.keys())

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
        fig = Figure(figsize=(5, 4), dpi=100)
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


class MainWindow(QMainWindow):
    def __init__(self, *args, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        self.comparisons = []
        self.isPopulated = {"colList1": False, "colList2": False, "compTable": False}

        uic.loadUi(MAIN_UI, self)
        self.setup_logger()
        self.dataset1_select_file_button = SelectFileButton(
            self.dataset1FileLoad, 1, self
        )
        self.dataset2_select_file_button = SelectFileButton(
            self.dataset2FileLoad, 2, self
        )

        # set up column select
        self.dataset1Columns_model = None
        self.dataset2Columns_model = None

        # set column buttons
        self.add_one_button = ColumnSelectButton(self.addOneButton, "add_one", self)
        self.add_all_button = ColumnSelectButton(self.addAllButton, "add_all", self)
        self.remove_one_button = ColumnSelectButton(
            self.removeOneButton, "remove_one", self
        )
        self.remove_all_button = ColumnSelectButton(
            self.removeAllButton, "remove_all", self
        )

        # set up dataframe tables
        self.dataframe1Table.horizontalHeader().setSectionResizeMode(
            QHeaderView.Interactive
        )
        self.dataframe1Table.horizontalHeader().setSectionsMovable(True)
        self.dataframe2Table.horizontalHeader().setSectionResizeMode(
            QHeaderView.Interactive
        )
        self.dataframe2Table.horizontalHeader().setSectionsMovable(True)

        # set up comparison table
        self.compTableModel = ComparisonTableModel(self.comparisons)
        self.comparisonColumnsTable.setModel(self.compTableModel)
        self.comparisonColumnsTable.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )

        # set up tabs column
        self.comparisonsTabLayout.setCurrentIndex(0)

        # set up compare and reset buttons
        self.compareButton.clicked.connect(self.compare)
        self.resetButton.clicked.connect(self.reset)

        # set up comparison output table
        self.comparisonTable.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )

        self.show()

    def _is_matching_type(self, col1, col2):
        global DATASET1
        global DATASET2
        if DATASET1[col1].data_type != DATASET2[col2].data_type:
            return False
        else:
            return True

    def _is_novel_comparison(self, comp_name):
        for comp in self.comparisons:
            if comp_name in comp[0]:
                return False
        return True

    def _update_setup(self):
        # update combo box
        self.comparisonsComboBox.clear()
        comp_names = [col[0] for col in self.comparisons]
        self.comparisonsComboBox.addItems(comp_names)

        # update compare and reset buttons
        if self.isPopulated["compTable"]:
            self.compareButton.setEnabled(True)
        else:
            self.compareButton.setEnabled(False)

    def create_plots(self, comp_df):
        rows = list(comp_df.index)
        grid_mtx = (
            [(0, i) for i in range(3)]
            + [(1, i) for i in range(3)]
            + [(2, i) for i in range(3)]
        )
        index = 0
        colors = ["c", "m"]
        for row in enumerate(rows):
            row_name = row[1]

            if row_name in NON_PLOT_ROWS:
                continue

            plot_model = Plot(self)
            try:
                comp_trimmed = comp_df.loc[:, comp_df.columns != "diff_col"].transpose()
                plot_model.ax.axes.bar(
                    x=list(comp_trimmed.index),
                    height=comp_trimmed[row_name].tolist(),
                    color=colors,
                )
                plot_model.ax.axes.set_title(row_name)
            except Exception as e:
                LOGGER.error("Encountered an error while creating plot")
                LOGGER.error(e)

            try:
                row_num = grid_mtx[index][0]
                column_num = grid_mtx[index][1]
                self.plotsGridLayout.addWidget(plot_model, row_num, column_num)
            except Exception as e:
                LOGGER.error("Encountered an error while adding plot")
                LOGGER.error(e)
                
            index += 1

    def compare(self):
        comp_name = self.comparisonsComboBox.currentText()
        col1, col2 = comp_name.split("-")
        compare_by_col = col1 == col2

        add_diff_col = self.addDiffCheckbox.isChecked()
        perform_validations = self.performValidationsCheckbox.isChecked()
        create_plots_checked = self.createVizCheckbox.isChecked()

        comp_df = None
        if DATASET1 != None and DATASET2 != None:
            comp_df = dc.compare_ds(
                col1=DATASET1[col1],
                col2=DATASET2[col2],
                perform_check=perform_validations,
                add_diff_col=add_diff_col,
                save_comp=False,
                compare_by_col=compare_by_col,
            )

            self.comp_table_model = ComparisonOutputTableModel(comp_df)
            self.comparisonTable.setModel(self.comp_table_model)
            self.resetButton.setEnabled(True)
        else:
            LOGGER.error("Datasets not available to make comparisons")

        if create_plots_checked and not comp_df.empty:
            self.create_plots(comp_df)

        self.comparisonsTabLayout.setCurrentIndex(1)

    def reset(self):
        # self.comparisonTable.setSpans()

        # self.resetButton.setEnabled(False)
        # self.comparisonsTabLayout.setCurrentIndex(1)
        pass

    def add_comparison(self):
        colList1_indexes = self.dataset1Columns.selectedIndexes()
        colList2_indexes = self.dataset2Columns.selectedIndexes()
        self.dataset1Columns.clearSelection()
        self.dataset2Columns.clearSelection()

        if len(colList1_indexes) < 1 or len(colList2_indexes) < 1:
            LOGGER.error("Two columns must be selected in order to create a comparison")
            return

        colList1_index = colList1_indexes[0]
        colList2_index = colList2_indexes[0]
        col1 = self.dataset1Columns_model.data(colList1_index)
        col2 = self.dataset2Columns_model.data(colList2_index)

        # make sure types match
        if not self._is_matching_type(col1, col2):
            LOGGER.error(
                "{} is of type {} and {} is of type {}. Comparisons must be of same type".format(
                    col1, DATASET1[col1].data_type, col2, DATASET2[col2].data_type
                )
            )
            return

        comp_name = "{}-{}".format(col1, col2)

        # make sure this is a novel comparison
        if not self._is_novel_comparison(comp_name):
            LOGGER.error("Comparison {} already exists".format(comp_name))
            return False

        self.comparisons.append([comp_name, col1, col2])
        self.comparisonColumnsTable.model().layoutChanged.emit()

        self.isPopulated["compTable"] = len(self.comparisons) > 0

        if self.isPopulated["compTable"]:
            self.remove_one_button.button.setEnabled(True)
            self.remove_all_button.button.setEnabled(True)

        self._update_setup()

    def add_comparisons(self):
        colList1_cols = self.dataset1Columns_model.cols
        colList2_cols = self.dataset2Columns_model.cols

        common_cols = list(set(colList1_cols).intersection(set(colList2_cols)))

        if len(common_cols) < 1:
            LOGGER.error("No common columns were found")
            return

        for col in common_cols:
            col1 = col
            col2 = col
            comp_name = "{}-{}".format(col1, col2)
            if not self._is_novel_comparison(comp_name):
                LOGGER.error("Comparison {} already exists".format(comp_name))
                continue
            self.comparisons.append([comp_name, col1, col2])
        self.comparisonColumnsTable.model().layoutChanged.emit()

        self.isPopulated["compTable"] = len(self.comparisons) > 0

        if self.isPopulated["compTable"]:
            self.remove_one_button.button.setEnabled(True)
            self.remove_all_button.button.setEnabled(True)
            self.add_all_button.button.setEnabled(False)

        self._update_setup()

    def remove_comparison(self):
        if not self.comparisonColumnsTable.selectionModel().hasSelection():
            LOGGER.error("Must select a row/rows to remove")
            return

        comp_indices = self.comparisonColumnsTable.selectionModel().selectedRows()

        for index in sorted(comp_indices):
            del self.comparisons[index.row()]
        self.comparisonColumnsTable.model().layoutChanged.emit()

        self.isPopulated["compTable"] = len(self.comparisons) > 0

        if not self.isPopulated["compTable"]:
            self.remove_one_button.button.setEnabled(False)
            self.remove_all_button.button.setEnabled(False)
            self.add_all_button.button.setEnabled(True)

        self._update_setup()

    def clear_comparisons(self):
        if not self.isPopulated["compTable"]:
            LOGGER.error("No rows to delete")
            return

        self.comparisons.clear()
        self.comparisonColumnsTable.model().layoutChanged.emit()

        self.isPopulated["compTable"] = len(self.comparisons) > 0

        if not self.isPopulated["compTable"]:
            self.remove_one_button.button.setEnabled(False)
            self.remove_all_button.button.setEnabled(False)
            self.add_all_button.button.setEnabled(True)

        self._update_setup()

    def setup_logger(self):
        font = QFont("Arial", 5)
        self.loggingBox.setFont(font)

        logHandler = LogStream(self)
        logHandler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        logging.getLogger().addHandler(logHandler)

    def render_data(self, dataset, ds_num):
        global DATASET1
        global DATASET2

        if ds_num == 1:
            DATASET1 = dataset

            # set columns
            self.dataset1Columns_model = DatasetColumnsListModel(DATASET1)
            self.dataset1Columns.setModel(self.dataset1Columns_model)

            self.isPopulated["colList1"] = (
                True if self.dataset1Columns_model.rowCount() > 0 else False
            )

            # set dataframe table
            self.dataframe1Table_model = DataframeTableModel(DATASET1.dataframe)
            self.dataframe1Table.setModel(self.dataframe1Table_model)
            self.ds_details_button1 = DatasetDetailsButton(
                self.datasetDetails1Button, dataset
            )
        if ds_num == 2:
            DATASET2 = dataset

            # set columns
            self.dataset2Columns_model = DatasetColumnsListModel(DATASET2)
            self.dataset2Columns.setModel(self.dataset2Columns_model)

            self.isPopulated["colList2"] = True if len(DATASET2.columns) > 0 else False

            # set dataframe table
            self.dataframe2Table_model = DataframeTableModel(DATASET2.dataframe)
            self.dataframe2Table.setModel(self.dataframe2Table_model)
            self.ds_details_button2 = DatasetDetailsButton(
                self.datasetDetails2Button, dataset
            )

        self.clear_comparisons()

        if self.isPopulated["colList1"] and self.isPopulated["colList2"]:
            self.add_one_button.button.setEnabled(True)
            self.add_all_button.button.setEnabled(True)
        else:
            self.add_one_button.button.setEnabled(False)
            self.add_all_button.button.setEnabled(False)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Data Comparator")

    window = MainWindow()
    app.exec_()
