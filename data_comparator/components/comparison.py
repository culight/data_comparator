"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    data model for Comparison object

### DEVELOPER NOTES:
"""
import logging
import itertools

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from components.dataset import Dataset, Column


_BAR_FIELDS = ['count', 'missing', 'unique']
_BOXPLOT_FIELDS = ['min', 'max', 'std', 'mean', 'zeros']

class Comparison:
    def __init__(self, col1, col2, compare_by_col):
        
        if not isinstance(col1.__class__, Column.__class__):
            print("ERROR: Column 1 must be a 'Column' object")
            
        if not isinstance(col2.__class__, Column.__class__):
            print("ERROR: Column 2 must be a 'Column' object")
            
        if col1.data_type != col2.data_type:
            print(
                "ERROR: {} is a {} and {} is a {}. They must be of matching types to be compared".format(
                    col1.name, col1.data_type, \
                     col2.name, col2.data_type
                )
            )
        print("\nComparing '{}' and '{}'... ".format(col1.name, col2.name))
        
        self.col1 = col1
        self.col2 = col2
        self.data_type = col1.data_type
        self.compare_by_col = compare_by_col

        if compare_by_col:
            self.name = col1.name
        else:
            if col1.name == col2.name:
                self.name = col1.ds_name + '.' + col1.name + '-' + col2.ds_name + '.' + col2.name
            else:
                self.name = col1.name + '-' + col2.name
        self.dataframe = None
    
    def set_dataframe(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def view(self):
        return self.dataframe
    
    def create_diff_column(self, checks_added: bool=False):
        assert self.col1 and self.col2, 'Two columns must be provided to create diff column'
        
        if checks_added:
            measures1 = {**self.col1.get_summary(), **self.col1.perform_check()}
            measures2 = {**self.col2.get_summary(), **self.col2.perform_check()}
            keys = list(self.col1.get_summary().keys()) + list(self.col1.perform_check().keys())
        else:
            measures1 = {**self.col1.get_summary()}
            measures2 = {**self.col2.get_summary()}
            keys = list(self.col1.get_summary().keys())

        diff_list = []
        for key in keys:
            try:
                diff = measures2[key] - measures1[key]
                if diff == 0:
                    diff = 'same'
            except:
                diff = 'diff' if measures1[key] != measures2[key] else 'same'
            diff_list.append(diff)
                
        return diff_list
            
    def plot(self):
        boxplot_created = False
        fig = plt.figure()
        ax = fig.add_subplot(111)
        plt.ion()
        
        fig.show()
        fig.canvas.draw()
        
        for i, measure in enumerate(self.dataframe.index):
            ax.clear()
            if measure in _BAR_FIELDS:
                ax.plot.bar(self.dataframe.loc[measure])
            elif measure in _BOXPLOT_FIELDS and not boxplot_created:
                col1_name = self.name.split('-')[0]
                col2_name = self.name.split('-')[1]
                _df = pd.DataFrame({
                    col1_name: self.col1.data,
                    col2_name: self.col2.data
                })
                _df.boxplot(ax=axes[1,1])
                boxplot_created = True
            fig.canvas.show()
                        