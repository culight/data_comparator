"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    GUI Application for Data Comparator

### DEVELOPER NOTES: To run this in parent directory - Enter "Make run" in console
"""

from os import mkdir
import sys
import logging
from jinja2 import Environment, PackageLoader, select_autoescape
import json
from pathlib import Path
import time

from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtPrintSupport import *
from PyQt5 import uic

import webbrowser

from data_comparator import data_comparator as dc
from .ui_models.utilities import *
from .ui_models.buttons import *
from .ui_models.tables import *
from .ui_models.lists import *
from .ui_models.combo_boxes import *

UI_DIR = Path(__file__).parent / "ui"
COMP_DIR = Path(__file__).parent / "components"
MAIN_UI_DIR = str(UI_DIR / "data_comparator.ui")
VALID_FILE_DIR = str(COMP_DIR / "validations_config.json")
NON_PLOT_ROWS = ["ds_name", "name", "data_type"]
HTML_TEMPLATE = UI_DIR / "template.html"
HTML_VALIDATIONS_TEMPLATE = UI_DIR / "template_validations.html"

DATASET1 = None
DATASET2 = None
VALIDS = {}

logging.basicConfig(
    stream=sys.stdout, format="%(asctime)`s` - %(message)s", level=logging.DEBUG
)
LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class MenuBar(QMenuBar):
    def __init__(self, menuBar, parent):
        super(MenuBar, self).__init__()
        self.parent = parent
        self.menuBar = menuBar
        self.comparisons = None
        self.actionNew = parent.actionNew
        self.actionReset = parent.actionReset
        self.actionExit = parent.actionExit
        self.actionCSV = parent.actionCSV
        self.actionParquet = parent.actionParquet
        self.actionJSON = parent.actionJSON
        self.actionSwapDatasets = parent.actionSwapDatasets
        self.actionExportHTMLReport = parent.actionExportHTMLReport
        self.actionIncludeValidations = parent.actionIncludeValidations

        # self.actionNew.triggered.connect(self.new)
        self.actionReset.triggered.connect(self.reset)
        self.actionExit.triggered.connect(self.exit)
        self.actionCSV.triggered.connect(self.export_to_csv)
        self.actionParquet.triggered.connect(self.export_to_parquet)
        self.actionJSON.triggered.connect(self.export_to_json)
        self.actionExportHTMLReport.triggered.connect(self.export_report)

    class ExportFile:
        def __init__(self, export_type, parent):
            self.comparisons = {}
            self.export_type = export_type
            self.parent = parent
            try:
                self.comparisons = self.parent.compare_all()
                assert len(self.comparisons) > 0
            except AssertionError:
                LOGGER.error("Cannot export - no comparison was found")

        def get_filepath(self):
            current_time = time.time()
            file_diag = QFileDialog()
            fname, _ = file_diag.getSaveFileName(
                self.parent,
                "QFileDialog.getSaveFileName()",
                str(current_time),
                "Data Files (*.{})".format(self.export_type),
            )
            if self.export_type != "html":
                output_path = Path(fname.replace("." + self.export_type, ""))
                output_path.mkdir(parents=True, exist_ok=True)
            else:
                output_path = Path(fname)
            return output_path

    def new(self):
        """
        Create new comparator
        """
        confirm_dialog = QMessageBox.question(
            self.parent,
            "Data Comparator",
            "A new Data Comparator session will be created",
            QMessageBox.Cancel | QMessageBox.Ok,
        )

        if confirm_dialog == QMessageBox.Ok:
            LOGGER.info("Creating new Comparator window")
            self.parent.__init__()
        else:
            pass

    def reset(self):
        """
        Reset the fields
        """
        confirm_dialog = QMessageBox.question(
            self.parent,
            "Data Comparator",
            "Currently loaded data will be deleted",
            QMessageBox.Cancel | QMessageBox.Ok,
        )

        if confirm_dialog == QMessageBox.Ok:
            LOGGER.info("Removing currently loaded data")
            self.parent.clear_comparisons()
            self.parent.clear_datasets()
        else:
            pass

    def exit(self):
        """
        exit the program
        """
        confirm_dialog = QMessageBox.question(
            self.parent,
            "Data Comparator",
            "Are you sure you want to quit?",
            QMessageBox.Yes | QMessageBox.No,
        )

        if confirm_dialog == QMessageBox.Yes:
            LOGGER.info("Exiting data comparator")
            sys.exit()
        else:
            pass

    def export_to_csv(self):
        """
        Export the comparison table to csv
        """
        csv_file = self.ExportFile(export_type="csv", parent=self.parent)
        try:
            file_path = csv_file.get_filepath()
            for comp_name, comp in csv_file.comparisons.items():
                comp_name_ext = comp_name + ".csv"
                file_name = file_path / comp_name_ext
                LOGGER.info("Saved file path: {}".format(file_name))
                comp.dataframe.to_csv(file_name)
        except Exception as e:
            LOGGER.error(str(e))

    def export_to_parquet(self):
        """
        Export the comparison table to parquet
        """
        parquet_file = self.ExportFile(export_type="parquet", parent=self.parent)
        try:
            file_path = parquet_file.get_filepath()
            for comp_name, comp in parquet_file.comparisons.items():
                comp_name_ext = comp_name + ".parquet"
                file_name = file_path / comp_name_ext

                df = comp.dataframe
                for col in df.columns:
                    df[col] = df[col].astype(str)
                LOGGER.info("Saved file path: {}".format(file_name))
                comp.dataframe.to_parquet(file_name)
        except Exception as e:
            LOGGER.error(str(e))

    def export_to_json(self):
        """
        Export the comparison table to json
        """
        json_file = self.ExportFile(export_type="json", parent=self.parent)
        try:
            file_path = json_file.get_filepath()
            for comp_name, comp in json_file.comparisons.items():
                comp_name_ext = comp_name + ".json"
                file_name = file_path / comp_name_ext
                LOGGER.info("Saved file path: {}".format(file_name))
                json_content = comp.dataframe.to_json(orient="split")
                json_output = json.loads(json_content)
                with open(file_name, "w") as f:
                    json.dump(json_output, f)
        except Exception as e:
            LOGGER.error(str(e))

    def export_report(self, validations=False):
        """
        Export custom report to HTML
        """
        env = Environment(
            loader=PackageLoader("data_comparator.ui"), autoescape=select_autoescape()
        )

        if validations:
            template = env.get_template("template_validations.html")
            self.parent.performValidationsCheckbox.checked = True
        else:
            template = env.get_template("template.html")
        html_file = self.ExportFile(export_type="html", parent=self.parent)

        try:
            file_path = html_file.get_filepath()
            f = open(file_path, "w")

            comp_paramters = {
                "comp_names": html_file.comparisons.keys(),
                "comparisons": {},
            }
            for comp_name, comp in html_file.comparisons.items():
                comp_dict = comp.dataframe.transpose().to_dict()
                comp_dict_proc = {}
                for name, value in comp_dict.items():
                    comp_dict_proc[name] = list(value.values())
                comp_paramters["comparisons"][comp_name] = comp_dict_proc

            html_output = template.render(
                comp_names=comp_paramters["comp_names"],
                comps=comp_paramters["comparisons"],
            )
            f.write(html_output)
            f.close()
            webbrowser.open_new_tab(file_path)
        except Exception as e:
            LOGGER.error(str(e))


class MainWindow(QMainWindow):
    """
    Main QT Window Class

    Args:
        QMainWindow: QMainWindow parent object
    """

    def __init__(self, *args, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)

        self.comparisons = []
        self.config_items = []
        self.config_names = []
        self.isPopulated = {
            "colList1": False,
            "colList2": False,
            "compList": False,
            "compTable": False,
        }
        self.comp_df = None

        uic.loadUi(MAIN_UI_DIR, self)
        QSettings("myorg", "myapp1").clear()
        QSettings("myorg", "myapp2").clear()

        # set up logger
        self.setup_logger()

        # set up menu bar
        self.menuBar.setNativeMenuBar(False)
        self.menuBarModel = MenuBar(self.menuBar, self)

        # set up dataset columns
        self.dataset1Columns.setAcceptDrops(True)
        self.dataset2Columns.setAcceptDrops(True)
        self.dataset1Columns_model = DatasetColumnsListModel(ds_num=1, parent=self)
        self.dataset2Columns_model = DatasetColumnsListModel(ds_num=2, parent=self)
        self.dataset1Columns.setModel(self.dataset1Columns_model)
        self.dataset2Columns.setModel(self.dataset2Columns_model)

        # set up select file buttons
        self.dataset1_select_file_button = SelectFileButton(
            self.dataset1FileLoad, 1, self
        )
        self.dataset2_select_file_button = SelectFileButton(
            self.dataset2FileLoad, 2, self
        )

        # set up config table
        self.config_items = self._read_json()
        self.config_names = [
            i["name"].replace(" ", "_").lower() for i in self.config_items
        ]
        self.configTableModel = ConfigTableModel(self.config_items)
        self.configTable.setModel(self.configTableModel)
        self.configTable.setItemDelegateForColumn(2, ComboBoxDelegate(self))
        self.configTable.setItemDelegateForColumn(3, LineEditDelegate(self, "value"))
        self.configTable.setItemDelegateForColumn(4, LineEditDelegate(self, "fields"))
        self.configTable.resizeColumnToContents(1)

        # set up input parameter table
        self.ip_button1 = InputParametersButton(self.inputParamsButton1, 1)
        self.ip_button2 = InputParametersButton(self.inputParamsButton2, 2)

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

        # set up compare and export buttons
        self.compareButton.clicked.connect(self.compare)
        self.exportValidationsButton.clicked.connect(self.export_validations)

        # set up comparison output table
        self.comparisonTable.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )

        self.show()

    def _active_menu_options_enabled(self, status):
        self.actionCSV.setEnabled(status)
        self.actionParquet.setEnabled(status)
        self.actionJSON.setEnabled(status)
        self.actionExportHTMLReport.setEnabled(status)
        self.actionIncludeValidations.setEnabled(status)
        # json

    def _passive_menu_options_enabled(self, status):
        self.actionReset.setEnabled(status)

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
        self.comparisonsComboBox.addItems(sorted(comp_names))

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
            "type": {"numeric": {}, "string": {}, "temporal": {}, "boolean": {}}
        }
        for entry in validation_data:
            vld_type = entry["type"].lower()
            vld_name = entry["name"].replace(" ", "_").lower()
            config_items["type"][vld_type][vld_name] = {
                "enabled": True if entry["enabled"] == "True" else False,
                "value": entry["value"],
                "fields": entry["fields"],
            }

        with open(VALID_FILE_DIR, "w") as write_file:
            json.dump(config_items, write_file)

        return config_items

    def _read_json(self):
        validation_data = None
        with open(VALID_FILE_DIR, "r") as read_file:
            validation_data = json.load(read_file)

        assert validation_data, LOGGER.error(
            "Error encountered while loading validations"
        )

        config_items = []
        for val_type, entries in validation_data["type"].items():
            for val_name, val_settings in entries.items():
                config_dict = {}
                config_dict["name"] = val_name.replace("_", " ").title()
                config_dict["type"] = val_type.title()
                config_dict["enabled"] = "True" if val_settings["enabled"] else "False"
                config_dict["value"] = val_settings["value"]
                config_dict["fields"] = val_settings["fields"]
                config_items.append(config_dict)

        return config_items

    def create_plots(self, data, is_profile=False):
        """
        Cceate plots for the select comparisons

        Args:
            data (Pandas DataFrame): data of interest
            is_profile (bool, optional): flag indicating a single column. Defaults to False.
        """
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
                    comp_trimmed = data.loc[:, data.columns != "diff_col"].transpose()
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
                    self.plotsGridLayout.addWidget(plot_model, row_num, column_num)
                except Exception as e:
                    LOGGER.error("Encountered an error while adding plot")
                    LOGGER.error(e)

                index += 1

    def export_validations(self):
        """Export the current validations content"""

        try:
            config_export_dialog = QMessageBox.about(
                self.exportValidationsButton,
                "Data Comparator",
                "Validations configuration has been exported.",
            )

        except Exception as e:
            LOGGER.error("Could not export validations configuration")
            LOGGER.error(str(e))

    def profile(self, col, ds):
        """
        provide profile info for one column

        Args:
            col (str): column name of interest
            ds (dataset): dataset of interest
        """
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
        self.exportButton.setEnabled(True)

        dtype = None
        try:
            dtype = profile.loc[["data_type"]].to_numpy()[0][0]
        except:
            LOGGER.error("Encountered an issue determining data type")

        self._clear_plots()
        if create_plots_checked and (dtype == "NumericColumn"):
            self.create_plots(ds.dataframe[col], is_profile=True)

        self.comparisonsTabLayout.setCurrentIndex(1)

    def compare(self, comp_name_external=None):
        """
        compare datasets of interest
        """

        # get comparison names
        comp_name = (
            comp_name_external
            if comp_name_external
            else self.comparisonsComboBox.currentText()
        )
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
        self.comp_df = None
        if DATASET1 != None and DATASET2 != None:
            # update validation settings
            self._write_json(self.configTableModel.data)

            self.comp_df = dc.compare_ds(
                col1=DATASET1[col1],
                col2=DATASET2[col2],
                perform_check=perform_validations,
                add_diff_col=add_diff_col,
                save_comp=True,
                compare_by_col=compare_by_col,
            )

            self.comp_table_model = ComparisonOutputTableModel(self.comp_df)
            self.comparisonTable.setModel(self.comp_table_model)

        else:
            LOGGER.error("Datasets not available to make comparisons")

        self._clear_plots()
        if create_plots_checked and not self.comp_df.empty:
            # remove validation fields
            if perform_validations:
                cols_to_drop = [
                    col for col in list(self.comp_df.index) if col in self.config_names
                ]
                if cols_to_drop:
                    self.comp_df = self.comp_df.drop(cols_to_drop)
            self.create_plots(self.comp_df)

        self.comparisonsTabLayout.setCurrentIndex(1)

    def compare_all(self):
        passive_comparisons = {}
        if self.comparisons:
            cached_comparisons = dc.get_comparisons()
            for c in self.comparisons:
                comp_name = c[0]
                if comp_name not in cached_comparisons:
                    self.compare(comp_name)

                comp_split = comp_name.split("-")
                if len(set(comp_split)) == 1:
                    # both columns have the same name -- use one
                    comp_name = comp_split[0]
                comparison = dc.get_comparison(comp_name)

                if comparison:
                    # only pull cached comparisons currently in list
                    passive_comparisons[comparison.name] = comparison
        else:
            LOGGER.warn("No comparisons to make")
        return passive_comparisons

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
            self._active_menu_options_enabled(True)
        self._update_setup()

    def add_comparisons(self):
        """
        add set of columns to list of active comparisons
        """
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
                    "{} is of type and {} is of type. Could not be compare".format(
                        col1, col2
                    )
                )
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
            self._active_menu_options_enabled(True)

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
            self._active_menu_options_enabled(False)

        self._update_setup()

    def clear_datasets(self):
        """
        remove loaded datasets and clear from list view
        """
        self._active_menu_options_enabled(False)
        self._passive_menu_options_enabled(False)
        self.comparisonTable.setModel(None)
        if self.dataset1Columns_model:
            self.dataset1Columns_model.reset()
            self.dataframe1Table.setModel(None)
            self.DATASET1 = None
        if self.dataset2Columns_model:
            self.dataset2Columns_model.reset()
            self.dataframe2Table.setModel(None)
            self.DATASET2 = None

    def clear_comparisons(self):
        """
        remove active comparisons
        """
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
            self._active_menu_options_enabled(False)

        self._update_setup()

    def setup_logger(self):
        """
        setup logging for this session
        """
        font = QFont("Arial", 5)
        self.loggingBox.setFont(font)

        logHandler = LogStream(self)
        logHandler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        logging.getLogger().addHandler(logHandler)

    def render_data(self, dataset, ds_num):
        """
        process the selected columns and display in info details section

        Args:
            dataset (dataset): dataset of interest
            ds_num (int): dataset index
        """
        global DATASET1
        global DATASET2

        if ds_num == 1:
            DATASET1 = dataset

            if DATASET1 == None:
                LOGGER.error("Dataset 1 was not sucessfully loaded")
                return

            # set columns
            self.dataset1Columns_model = DatasetColumnsListModel(
                dataset=DATASET1, ds_num=1, parent=self
            )
            self.dataset1Columns.setModel(self.dataset1Columns_model)

            self.isPopulated["colList1"] = True if len(DATASET1.columns) > 0 else False

            # set dataframe table
            self.dataframe1Table_model = DataframeTableModel(DATASET1.dataframe)
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
            self.dataset2Columns_model = DatasetColumnsListModel(
                dataset=DATASET2, ds_num=2, parent=self
            )
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
            self._passive_menu_options_enabled(True)
        else:
            self.add_one_button.button.setEnabled(False)
            self.add_all_button.button.setEnabled(False)
            self._passive_menu_options_enabled(False)


def main(*args, **kwargs):
    import pkg_resources

    app = QApplication(sys.argv)
    app.setApplicationName("Data Comparator")

    version = pkg_resources.require("data-comparator")[0].version
    window_title = "Data Comparator" + " - " + version

    window = MainWindow()
    window.setWindowTitle(window_title)
    app.exec_()


if __name__ == "__main__":
    sys.exit(main())
