from components.dataset import Dataset

class DataCupboard(object):
    def __init__(self):
        self.entry_types = (
            'dataset',
            'comparison',
            'comp_dataframes',
            'profiles'
        )

        self.components = {
            'dataset': {},
            'comparison': {},
            'comp_df': {},
            'profile': {}
        }

    def write_data(self, entry_type: str, entry_name: str, entry):
        """Update persisted data dictionaries"""
        assert entry_type, 'entry type not provided'
        assert entry_name, '{} entry name not provided'.format(entry_type)
        try:
            self.components[entry_type][entry_name] = entry
        except Exception as e:
            print(e)

    def read_data(self, entry_type: str, entry_name=None):
        """Read persisted data dictionaries"""
        assert entry_type, 'entry type not provided'
        output = {}
        if entry_name:
            try:
                data = self.components[entry_type][entry_name]
                output = {entry_name: data}
            except Exception as e:
                print(e)
            
        else:
            output = {name: data for (name, data) in self.components[entry_type].items()}
   
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