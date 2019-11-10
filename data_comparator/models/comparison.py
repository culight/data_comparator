"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    data model for Comparison object

### DEVELOPER NOTES:
"""
import logging
import pandas as pd
from models.dataset import Dataset, Column

class Comparison:
    def __init__(self, col1, col2):
        
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
        print("Initializing comparison for '{}' and '{}'... ".format(col1.name, col2.name))
        
        self.col1 = col1
        self.col2 = col2
        self.name = col1.name + '-' + col2.name
    
