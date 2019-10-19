import pandas as pd

# import sample dataset
raptors_hist_path = r"H:\repos\data-comparator\tests\test_data\nba-raptor\historical_RAPTOR_by_player.csv"
raptors_modrn_path = r"H:\repos\data-comparator\tests\test_data\nba-raptor\modern_RAPTOR_by_player.csv"
ds_hist = Dataset(raptors_hist_path)
ds_modrn = Dataset(raptors_modrn_path)

ds_hist.prepare_columns()
ds_modrn.prepare_columns()

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
