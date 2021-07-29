# Data Comparator

## Overview

Data Comparator is a pandas-based data profiling tool for quick and modular profiling of two datasets. The primary inspiration for this project was quickly comparing two datasets from a number of different formats after some transformation was applied, but a range of capabilities have/will continue to been implemented.

Data Comparator would be useful for the following scenarios:

- Compare old/new (or original/modified) datasets to find general differences
- Routine EDA of a dataframe
- Compare two datasets of different formats
- Profile a dataset during interactive debugging
- Compare various columns within the same dataset
- Check for specific abnormalities within a dataset
- Export a comparison in HTML form

## Setup

Use [pip](https://pip.pypa.io/en/stable/) to install the Data Comparator package:

### Installation

```
pip install data_comparator
```

### Running

A command line interface and graphical user interface are provided.

#### Command Line:

```
import data_comparator.data_comparator as dc
```

#### GUI:

```
make run
```

![gui data loading image](https://github.com/culight/data_comparator/blob/master/docs/examples/general1.png)
![gui data detail exmaple](https://github.com/culight/data_comparator/blob/master/docs/examples/general2.png)

Export a comparison to an HTML report:
![gui export tab](https://github.com/culight/data_comparator/blob/master/docs/examples/export1.png)
![gui htmp report](https://github.com/culight/data_comparator/blob/master/docs/examples/export2.png)

## Usage

User can load, profile, validate, and compare datasets as shown below. For the sake of example, I'm using a dataset that provides historical avocado prices.

### Loading data

Data can be loaded from a file or dropped into the data column boxes in the _Data Loading_ tab in the GUI. Note that the loading will happen automatically, so carefully drop the files _directly_ into the desired box.

#### Load From a File

```
avo2020_dataset = dc.load_dataset(avo_path / "avocado2020.csv", "avo2020")
```

#### Load from a (Pandas or Spark) dataframe

```
avo2019_dataset = dc.load_dataset(avocado2019_df, "avo2019")
```

#### Load With Input Parameters

```
avo2020_adj_dataset = dc.load_dataset(
    data_source=avoPath / "avo2020_adjusted.parquet,
    data_source_name="avo2020_adjusted",
    engine="fastparquet",
    columns=["Date", "AveragePrice", Volume", "year"]
)
```

Note that [PyArrow](https://arrow.apache.org/docs/index.html) is the default engine for reading parquets in Data Comparator.

#### Load Multiple Datasets

```
avo2017_path = avoPath / "avocado2017.sas7bdat"
avo2018_path = avoPath / "avocado2018.sas7bdat"

avo2017_ds, avo2018_ds = avo2018_dsdc.load_datasets(
    avo2017_path,
    avo2018_path,
    data_source_names=["avo2017", "avo2018"],
    load_params_list=[{},{"iterator":True, "chunksize":1000}]
)
```

In the snippet above, I'm reading in the 2017 SAS file as is, and reading the 2018 one incrementally - 1000 lines at a time.

### Comparing Data

Data from various types can be compared with user-specified columns or all identically-named columns between the datasets. The comparisons are automatically saved for each session.

#### Compare Datasets

```
avo2020_ds = dc.getDataset("avo2020")
avo2020_adj_ds = dc.getDataset("avo2020_adjusted)

dc.compare_ds(avo2019_ds, avo2020_adj_ds)
```

#### Compare Files

```
dc.compare(
    avo_path / "avocado2020.csv",
    avo_path / "avo2020_adjusted.parquet"
)
```

#### Example Output

![comparison exmaple](https://github.com/culight/data_comparator/blob/master/docs/examples/compare_example.png)

### Other Features

Some metadata for each dataset/comparison object is provided. Here, I use a cosmetic product dataset to illustrate some use cases.

#### Quick Dataset Summary

Basic metadata and summary information is provided for the dataset object.

```
skin_care_ds = dc.get_dataset("skin_care")
skin_care_ds.get_summary()

{'path': PosixPath('/path/to/cosmetics_data/skinproduct_vfdemo.sas7bdat'),
 'format': 'sas7bdat',
 'size': '13.56 MB',
 'columns': {'ProductKey': <components.dataset.StringColumn at 0x7f9a05442d30>,
  'DistributionCenter': <components.dataset.StringColumn at 0x7f9a0543fe80>,
  'DATE_CHAR': <components.dataset.StringColumn at 0x7f9a021ac820>,
  'Discount': <components.dataset.NumericColumn at 0x7f9a085c5490>,
  'Revenue': <components.dataset.NumericColumn at 0x7f9a085c5280>},
 'ds_name': 'skin_care',
 'load_time': '0:00:01.062732'}
```

The dataset object is subscriptable, so you can access individual columns as a subscript. We're accessing the summary for the _Revenue_ column in the snippet below.

```
skin_care_ds["Revenue"].get_summary()

{'ds_name': 'skin_care',
'name': 'Revenue',
'count': 147070,
'missing': 0,
'data_type': 'NumericColumn',
'min': 0.0,
'max': 1045032.0,
'std': 118382.93241134178,
'mean': 79200.74877269327,
'zeros': 1433}
```

#### Perform Checks

I've added some basic data validations for various data types. Use the _perform_checks()_ method to perform the validations. Note that String type comparisons can be computationally expensive; consider using the row_limit flag when perform checks on columns of String type.

```
skin_care_ds["Revenue"].perform_check()

{'pot_outliers': '4035',
 'susp_skewness': '2.939470744411452',
 'susp_zero_count': ''}
```

I'm still working out the kinks with some of the checks (numeric checks, like above, to be exact).
Check the _src/validation_config.json_ to manage validations.

## Coming Attractions

Updates and fixes (mostly [here](https://github.com/culight/data_comparator/issues)) will be forthcoming. This was a random project that I started for my own practical use in the field, so I'm certainly open to collaboration/feedback. You can drop a comment or find my email below.

## Authors

- Demerrick Moton (dmoton3.14@gmail.com)
