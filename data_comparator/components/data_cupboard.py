"""
### CODE OWNERS: Demerrick Moton
### OBJECTIVE:
    data model for data file object
### DEVELOPER NOTES:
"""
import logging
import os
from .dataset import Dataset

logging.basicConfig(
    format="%(asctime)s - %(message)s", level=os.environ.get("LOGLEVEL", "INFO")
)
LOGGER = logging.getLogger(__name__)


# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class DataCupboard(object):
    def __init__(self):
        self.entry_types = ("dataset", "comparison",
                            "comp_dataframes", "profiles")

        self.components = {
            "dataset": {},
            "comparison": {},
            "comp_df": {},
            "profile": {},
        }

    def write_data(self, entry_type: str, entry_name: str, entry):
        """Update persisted data dictionaries"""
        try:
            assert entry_type
        except AssertionError as err:
            LOGGER.exception("entry type not provided")
            raise err
        try:
            assert entry_name
        except AssertionError as err:
            LOGGER.exception("{} entry name not provided".format(entry_type))
            raise err

        try:
            self.components[entry_type][entry_name] = entry
        except Exception as err:
            LOGGER.exception(err)

    def read_data(self, entry_type: str, entry_name=None):
        """Read persisted data dictionaries"""
        try:
            assert entry_type
        except Exception as err:
            LOGGER.exception("entry type not provided")
            raise err

        output = {}
        if entry_name:
            try:
                data = self.components[entry_type][entry_name]
                output = data
            except Exception as err:
                LOGGER.exception(err)

        else:
            output = {
                name: data for (name, data) in self.components[entry_type].items()
            }

        return output

    def remove_data(self, entry_type=None, entry_name=None):
        if not entry_type and not entry_name:
            # clear everything
            for comp_type in self.components.keys():
                self.components[comp_type].clear()
        elif entry_type and not entry_name:
            # clear entries of a specific component
            self.components[entry_type].clear()
        elif entry_type and entry_name:
            # remove a specific entry
            self.components[entry_type][entry_name].clear()
        else:
            print("Please provide entry type")

    def pop_data(self, entry_type=None, entry_name=None):
        output = None
        if not entry_type and not entry_name:
            # clear everything
            for comp_type in self.components.keys():
                output[comp_type] = self.components[comp_type].copy()
                self.components[comp_type].clear()
        elif entry_type and not entry_name:
            # clear entries of a specific component
            output = self.components[entry_type].copy()
            self.components[entry_type].clear()
        elif entry_type and entry_name:
            # remove a specific entry
            output = self.components[entry_type][entry_name].copy()
            self.components[entry_type][entry_name].clear()
        else:
            LOGGER.warning("Please provide entry type")
        return output
