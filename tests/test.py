import pandas as pd
from models import Dataset, Comparison

# import sample dataset
raptors_hist_path = r"H:\repos\data-comparator\test_data\nba-raptor\historical_RAPTOR_by_player.csv"
raptors_modrn_path = r"H:\repos\data-comparator\test_data\nba-raptor\modern_RAPTOR_by_player.csv"
raptors_hist_dataset = Dataset(raptors_hist_path)
raptors_modrn_dataset = Dataset(raptors_modrn_path)

raptors_hist_dataset.prepare_columns()
raptors_modrn_dataset.prepare_columns()

raptors_hist_dataset.columns
raptors_modrn_dataset.columns

hist_df = raptors_hist_dataset.dataframe
modrn_df = raptors_modrn_dataset.dataframe

col_a = raptors_hist_dataset.columns['mp']
col_b = raptors_modrn_dataset.columns['mp']

col_a.max
col_b.min

comp1 = Comparison(col_a, col_b)
comp1
