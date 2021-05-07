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

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.figure import Figure
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg,
)

import data_comparator.data_comparator as dc

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
