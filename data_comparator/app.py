"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    GUI Application for Data Comparator

### DEVELOPER NOTES:
"""
from logging import Logger

from pandas.core.algorithms import value_counts
from components.dataset import Dataset
import sys
import logging
from pathlib import Path
import json

from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtPrintSupport import *
from PyQt5 import uic

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.figure import Figure
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg,
    NavigationToolbar2QT as NavigationToolbar,
)

from view_models import *

import data_comparator as dc

MAIN_UI = "ui/data_comparator.ui"
DETAIL_DLG = "ui/data_detail_dialog.ui"
INPUT_PARAMS_DLG = "ui/input_parameters_dialog.ui"
VALID_FILE = "components/validations_config.json"
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


class MainWindow(QMainWindow):
    def __init__(self, *args, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        self.comparisons = []
        self.config_items = []
        self.config_names = []
        self.isPopulated = {"colList1": False, "colList2": False,
                            "compList": False, "compTable": False}

        uic.loadUi(MAIN_UI, self)
        QSettings('myorg', 'myapp1').clear()
        QSettings('myorg', 'myapp2').clear()

        # set up logger
        self.setup_logger()

        # set up select file buttons
        self.dataset1_select_file_button = SelectFileButton(
            self.dataset1FileLoad, 1, self
        )
        self.dataset2_select_file_button = SelectFileButton(
            self.dataset2FileLoad, 2, self
        )

        # set up config table
        self.config_items = self._read_json()
        self.config_names = [i['name'].replace(
            ' ', '_').lower() for i in self.config_items]
        self.configTableModel = ConfigTableModel(self.config_items)
        self.configTable.setModel(self.configTableModel)
        self.configTable.setItemDelegateForColumn(2, ComboBoxDelegate(self))
        self.configTable.setItemDelegateForColumn(
            3, LineEditDelegate(self, 'value'))
        self.configTable.setItemDelegateForColumn(
            4, LineEditDelegate(self, 'fields'))
        self.configTable.resizeColumnToContents(1)

        # set up input parameter table
        self.ip_button1 = InputParametersButton(
            self.inputParamsButton1, 1)
        self.ip_button2 = InputParametersButton(
            self.inputParamsButton2, 2)

        # set up column select
        self.remove_one_button = ColumnSelectButton(
            self.removeOneButton, "remove_one", self
        )
        self.remove_all_button = ColumnSelectButton(
            self.removeAllButton, "remove_all", self
        )
        self.dataset1Columns_model = None
        self.dataset2Columns_model = None

        # set column buttons
        self.add_one_button = ColumnSelectButton(
            self.addOneButton, "add_one", self)
        self.add_all_button = ColumnSelectButton(
            self.addAllButton, "add_all", self)
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
        if self.isPopulated["compList"]:
            self.compareButton.setEnabled(True)
        else:
            self.compareButton.setEnabled(False)

    def _clear_plots(self):
        for index in reversed(range(self.plotsGridLayout.count())):
            self.plotsGridLayout.itemAt(index).widget().setParent(None)

    def _write_json(self, validation_data):
        assert validation_data, LOGGER.error("Validation data not found")

        config_items = {
            'type':
            {
                'numeric': {},
                'string': {},
                'temporal': {},
                'boolean': {}
            }
        }
        for entry in validation_data:
            vld_type = entry['type'].lower()
            vld_name = entry['name'].replace(' ', '_').lower()
            config_items['type'][vld_type][vld_name] = {
                'enabled': True if entry['enabled'] == 'True' else False,
                'value': entry['value'],
                'fields': entry['fields']
            }

        with open(VALID_FILE, "w") as write_file:
            json.dump(config_items, write_file)

        return config_items

    def _read_json(self):
        validation_data = None
        with open(VALID_FILE, "r") as read_file:
            validation_data = json.load(read_file)

        assert validation_data, LOGGER.error(
            "Error encountered while loading validations"
        )

        config_items = []
        for val_type, entries in validation_data["type"].items():
            for val_name, val_settings in entries.items():
                config_dict = {}
                config_dict["name"] = val_name.replace('_', ' ').title()
                config_dict["type"] = val_type.title()
                config_dict["enabled"] = 'True' if val_settings["enabled"] else 'False'
                config_dict["value"] = val_settings["value"]
                config_dict["fields"] = val_settings["fields"]
                config_items.append(config_dict)

        return config_items

    def create_plots(self, data, is_profile=False):
        if is_profile:
            plot_model = Plot(self)
            plot_model.ax.axes.boxplot(data)
            self.plotsGridLayout.addWidget(plot_model, 0, 0)
        else:
            rows = list(data.index)
            colors = ["c", "m"]
            grid_mtx = (
                [(0, i) for i in range(3)]
                + [(1, i) for i in range(3)]
                + [(2, i) for i in range(3)]
            )
            index = 0
            for row in rows:
                row_name = row
                if row_name in NON_PLOT_ROWS:
                    continue

                plot_model = Plot(self)
                try:
                    comp_trimmed = data.loc[:,
                                            data.columns != "diff_col"].transpose()
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
                    plot_model.setSizePolicy(
                        QSizePolicy.Expanding, QSizePolicy.Expanding
                    )
                    self.plotsGridLayout.addWidget(
                        plot_model, row_num, column_num)
                except Exception as e:
                    LOGGER.error("Encountered an error while adding plot")
                    LOGGER.error(e)

                index += 1

    def profile(self, col, ds):
        perform_validations = self.performValidationsCheckbox.isChecked()
        create_plots_checked = self.createVizCheckbox.isChecked()

        profile = dc.profile(ds[col])

        try:
            dtype = profile.loc[["data_type"]][0][0]
            print("type is: ", dtype)
        except Exception as e:
            print(e)

        self.comp_table_model = ComparisonOutputTableModel(profile)
        self.comparisonTable.setModel(self.comp_table_model)
        self.resetButton.setEnabled(True)

        dtype = None
        try:
            dtype = profile.loc[["data_type"]].to_numpy()[0][0]
        except:
            LOGGER.error("Encountered an issue determining data type")

        self._clear_plots()
        if create_plots_checked and (dtype == "NumericColumn"):
            self.create_plots(ds.dataframe[col], is_profile=True)

        self.comparisonsTabLayout.setCurrentIndex(1)

    def compare(self):

        # start with clean slate
        self.reset()

        # get comparison names
        comp_name = self.comparisonsComboBox.currentText()
        col1, col2 = comp_name.split("-")

        # is this a profiling combination?
        is_profile = (col1 == "=====") | (col2 == "=====")
        if is_profile:
            col_info = (col1, DATASET1) if "==" in col2 else (col2, DATASET2)
            self.profile(col_info[0], col_info[1])
            return

        # retreive comparison settings
        compare_by_col = col1 == col2
        add_diff_col = self.addDiffCheckbox.isChecked()
        perform_validations = self.performValidationsCheckbox.isChecked()
        create_plots_checked = self.createVizCheckbox.isChecked()

        # make comparisons
        comp_df = None
        if DATASET1 != None and DATASET2 != None:
            # update validation settings
            self._write_json(self.configTableModel.data)

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

        self._clear_plots()
        if create_plots_checked and not comp_df.empty:
            # remove validation fields
            if perform_validations:
                cols_to_drop = [col for col in list(
                    comp_df.index) if col in self.config_names]
                if cols_to_drop:
                    comp_df = comp_df.drop(cols_to_drop)
            self.create_plots(comp_df)

        self.comparisonsTabLayout.setCurrentIndex(1)

    def reset(self):
        # clear table
        self._clear_plots()
        if self.isPopulated['compTable']:
            self.comp_table_model.clear()
        dc.clear_comparisons()
        self.resetButton.setEnabled(False)

    def add_comparison(self):
        colList1_indexes = self.dataset1Columns.selectedIndexes()
        colList2_indexes = self.dataset2Columns.selectedIndexes()
        self.dataset1Columns.clearSelection()
        self.dataset2Columns.clearSelection()

        if len(colList1_indexes) < 1 or len(colList2_indexes) < 1:
            LOGGER.error(
                "Two columns must be selected in order to create a comparison")
            return

        colList1_index = colList1_indexes[0]
        colList2_index = colList2_indexes[0]
        col1 = self.dataset1Columns_model.data(colList1_index)
        col2 = self.dataset2Columns_model.data(colList2_index)

        is_profile = (col1 == "=====") | (col2 == "=====")
        null_case = (col1 == "=====") & (col2 == "=====")

        if null_case:
            LOGGER.error("Not a valid comparison/profiling option")
            return

        # make sure types match
        if not is_profile:
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

        self.isPopulated["compList"] = len(self.comparisons) > 0

        if self.isPopulated["compList"]:
            self.remove_one_button.button.setEnabled(True)
            self.remove_all_button.button.setEnabled(True)

        self._update_setup()

    def add_comparisons(self):
        colList1_cols = self.dataset1Columns_model.cols[1:]
        colList2_cols = self.dataset2Columns_model.cols[1:]

        common_cols = list(set(colList1_cols).intersection(set(colList2_cols)))

        if len(common_cols) < 1:
            LOGGER.error("No common columns were found")
            return

        for col in common_cols:
            col1 = col
            col2 = col
            if DATASET1[col1].data_type != DATASET2[col2].data_type:
                LOGGER.error(
                    "{} is of type and {} is of type. Could not be compare".format(col1, col2))
                continue
            comp_name = "{}-{}".format(col1, col2)
            if not self._is_novel_comparison(comp_name):
                LOGGER.error("Comparison {} already exists".format(comp_name))
                continue
            self.comparisons.append([comp_name, col1, col2])
        self.comparisonColumnsTable.model().layoutChanged.emit()

        self.isPopulated["compList"] = len(self.comparisons) > 0

        if self.isPopulated["compList"]:
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

        self.isPopulated["compList"] = len(self.comparisons) > 0

        if not self.isPopulated["compList"]:
            self.remove_one_button.button.setEnabled(False)
            self.remove_all_button.button.setEnabled(False)
            self.add_all_button.button.setEnabled(True)

        self._update_setup()

    def clear_comparisons(self):
        if not self.isPopulated["compList"]:
            LOGGER.error("No rows to delete")
            return

        self.comparisons.clear()
        self.comparisonColumnsTable.model().layoutChanged.emit()

        self.isPopulated["compList"] = len(self.comparisons) > 0

        if not self.isPopulated["compList"]:
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

            if DATASET1 == None:
                LOGGER.error("Dataset 1 was not sucessfully loaded")
                return

            # set columns
            self.dataset1Columns_model = DatasetColumnsListModel(DATASET1)
            self.dataset1Columns.setModel(self.dataset1Columns_model)

            self.isPopulated["colList1"] = True if len(
                DATASET1.columns) > 0 else False

            # set dataframe table
            self.dataframe1Table_model = DataframeTableModel(
                DATASET1.dataframe)
            self.dataframe1Table.setModel(self.dataframe1Table_model)
            self.ds_details_button1 = DatasetDetailsButton(
                self.datasetDetails1Button, dataset
            )

        if ds_num == 2:
            DATASET2 = dataset

            if DATASET2 == None:
                LOGGER.error("Dataset 2 was not sucessfully loaded")
                return

            # set columns
            self.dataset2Columns_model = DatasetColumnsListModel(DATASET2)
            self.dataset2Columns.setModel(self.dataset2Columns_model)

            self.isPopulated["colList2"] = True if len(
                DATASET2.columns) > 0 else False

            # set dataframe table
            self.dataframe2Table_model = DataframeTableModel(
                DATASET2.dataframe)
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
