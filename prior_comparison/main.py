import data_comparator.data_comparator as dc
from pyspark.sql import SparkSession
from pathlib import Path
import pandas as pd

FLIGHT_DATA = Path(
    r"/Users/dmoton/projects/data_projects/data_comparator/prior_comparison/data"
)
 
"""
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
"""

spark = SparkSession.builder.appName('prior_comparison_test').getOrCreate()

airline2013 = Path(FLIGHT_DATA / 'On_Time_On_Time_Performance_2013_1.csv')
airline2014 = Path(FLIGHT_DATA / 'On_Time_On_Time_Performance_2014_1.csv')
airline2015 = Path(FLIGHT_DATA / 'On_Time_On_Time_Performance_2013_1.csv')

airline2013_df = spark.read.csv(str(airline2013), header=True)
airline2013_df.write.parquet('airline2013.parquet')

pd_df = pd.DataFrame()
for index, col in enumerate(airline2013_df.columns[:5]):
    print(col)
    col_df = airline2013_df.select(col).toPandas()
    pd_df = pd.concat([pd_df, col_df], axis=1)

ds_2013 = dc.load_dataset(data_source=airline2013, data_source_name='2013')