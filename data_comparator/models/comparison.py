"""
### CODE OWNERS: Demerrick Moton

### OBJECTIVE:
    data model for Comparison object

### DEVELOPER NOTES:
"""
from models.dataset import Dataset

class Comparison:
    col_a = None
    col_b = None

    def __init__(self, col_a, col_b):
        self.col_a = col_a
        self.col_b = col_b
