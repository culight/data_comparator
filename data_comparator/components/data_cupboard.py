import shelve
import os
import shutil
import pickle
from pathlib import Path


class DataCupboard(object):
    def __init__(self, db_path=None):

        self.entry_types = (
            'dataset',
            'comparison',
            'comp_dataframes',
            'profiles'
        )

        self.db_path = "db/" if not db_path else db_path
        for entry_type in self.entry_types:
            entry_type_path = self.db_path + entry_type
            if not os.path.exists(entry_type_path):
                os.makedirs(entry_type_path)
            else:
                shutil.rmtree(entry_type_path)
                os.makedirs(entry_type_path)
        
    def write_data(self, entry_type: str, entry_name: str, entry):
        assert entry_type, 'entry type not provided'
        assert entry_name, '{} entry name not provided'.format(entry_type)

        entry_filepath = self.db_path + '/' + entry_type + '/' + entry_name
        entry_file = open(entry_filepath, 'ab')

        try:
            pickle.dump(entry, entry_file)
        except (pickle.PickleError, Exception) as e:
            print(e)
        finally:
            entry_file.close()

    def read_data(self, entry_type: str, entry_name=None):
        assert entry_type, 'entry type not provided'
        data = None
        if entry_name:
            entry_filepath = self.db_path + '/' + entry_type + '/' + entry_name
            entry_file = open(entry_filepath, 'rb')
            try:
                data = pickle.load(entry_file)
                entry_file.close()
            except (pickle.PickleError, Exception) as e:
                print(e)
            finally:
                entry_file.close()
            
        else:
            entry_filepath = self.db_path + '/' + entry_type
            data = list()

            entry_files = [f for f in Path(entry_filepath).glob('*')]
            for entry_filepath in entry_files:
                entry_file = open(entry_filepath, 'rb')
                try:
                    item = pickle.load(entry_file)
                    data.append(item)
                except (EOFError, Exception):
                    break
                entry_file.close()
            
        return data
    
    def remove_data(self, entry_type=None, entry_name=None):
        if not entry_type and not entry_name:
            # remove everything
            for entry_type in self.entry_types:
                entry_type_path = self.db_path + '/' + entry_type
                if os.path.exists(entry_type_path):
                    shutil.rmtree(entry_type_path)
                    os.makedirs(entry_type_path)
        elif entry_type and not entry_name:
            entry_type_path = self.db_path + '/' + entry_type
            if os.path.exists(entry_type_path):
                shutil.rmtree(entry_type_path)
                os.makedirs(entry_type_path)
        elif entry_type and entry_name:
            entry_type_path = self.db_path + '/' + entry_type + '/' + entry_name
            if os.path.exists(entry_type_path):
                shutil.rmtree(entry_type_path)
                os.makedirs(entry_type_path)
        else:
            print("Please provide entry type")