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


class ComparisonsComboBox(QComboBox):
    def __init__(self, comparisons, parent=None):
        super(ComparisonsComboBox, self).__init__(parent)
        self.comparisons = comparisons