"""
Base processing of data function

author: Dmitriy Grigorev
"""

import os
import time
from typing import Any, List, Optional

import hydra
import vaex
from omegaconf import DictConfig

from src.common.functions import (
    DEBUG,
    add_join_column,
    calculate_elapsed_time,
    empty_dataframe,
    hdf5_file_name,
    out_file_name,
    raw_file_name,
)

global HYDRA_CONFIG
HYDRA_CONFIG = None


def convert_csv(
    file=None, chunksize=5_000_000, names=None, nrows=None
) -> vaex.dataframe.DataFrame:
    """Convert CSV data to HDF5 format"""
    print(f'Converting the file "{file}" from CSV to HDF5 format...')
    return vaex.from_csv(
        file,
        convert=True,
        chunk_size=chunksize,
        progress=True,
        names=names,
        nrows=nrows,
    )


def open_hdf5_data(file=None, nrows=None) -> vaex.dataframe.DataFrame:
    """Open the HDF5 file"""
    df_open = vaex.open(file)
    if not DEBUG and len(df_open) == nrows:
        raise IOError(
            f'Attention the file "{file}" consist data only for first {nrows} records and used to the DEBUG mode. '
            f"Please, remove file and restart data processing."
        )

    return df_open


def process_grouping_data(
    df: vaex.dataframe.DataFrame, groupby: List = []
) -> vaex.dataframe.DataFrame:
    # Grouping and counting real estate data
    df_group = df.groupby(by=groupby, progress=True).agg(
        {"paon_count": "count"}
    )
    df_having = df_group[df_group["paon_count"] > 2].drop(["paon_count"])

    return df_having


def process_joining_data(
    df: vaex.dataframe.DataFrame,
    dh: vaex.dataframe.DataFrame,
    groupby=[],
) -> vaex.dataframe.DataFrame:
    """Creates a `new` DataFrame as result as join `source` DataFrame with `group` DataFrame.
       Before a join operation into source data frame will be add a new column which contain the
       same column set as was used in group by expression. New column use as key in
       joining operation.

    :param vaex.dataframe.DataFrame df: Source DataFrame for joining.
    :param vaex.dataframe.DataFrame dh: The  DataFrame has contained column as result operation group by.
    :param list groupby: List of columns used in group by.
    """
    start_time = time.time()
    if len(dh) > 0:
        print("Start join opereation...")
        # Add join column
        dc, join_col_name = add_join_column(dh)
        dj = df.join(dc, on=join_col_name, how="inner", rsuffix="_r")
        # Remove N/A rows and drop unused columns
        df = dj.dropna()
        # df = dj[~dj[f'{join_col_name}_r'].isna()]
        df = df.drop([x + "_r" for x in groupby])
        print(
            f"Total elapsed time for join data: {calculate_elapsed_time(start_time)}"
        )
        # Drop temporary columns after join operation
        df = df.drop([x for x in [join_col_name, f"{join_col_name}_r"]])
    else:
        # If no result, just return empty dataframe
        df = empty_dataframe(HYDRA_CONFIG)
    return df


def process_export_data(
    df: vaex.dataframe.DataFrame,
    file: str,
    chunksize: int,
) -> None:
    rows_to_export = len(df)
    if rows_to_export > 0:
        print(f"The result set has contained {rows_to_export} rows of data.")
        # The chunk_size parameter reduce memory and the parallel parameter allows use more than one core of processor
        df.export_csv(
            file,
            progress=True,
            chunk_size=chunksize,
            parallel=True,
            header=False,
            encoding="utf-8",
            # compression='gzip'
        )
    else:
        print("The result set is empty. No data will be exported.")


@hydra.main(config_path="../config", config_name="main")
def process_data(config: DictConfig):
    """Function to process the data"""
    HYDRA_CONFIG = config

    if DEBUG:
        print("Process run in DEBUG mode.")
        chunksize = config.raw.chunksize
        nrows = config.raw.nrows
    else:
        chunksize = config.process.chunksize
        nrows = config.process.nrows

    raw_file = raw_file_name(HYDRA_CONFIG)
    hdf5_file = hdf5_file_name(HYDRA_CONFIG)
    out_file = out_file_name(HYDRA_CONFIG)

    if not os.path.isfile(hdf5_file):
        convert_csv(
            file=raw_file,
            chunksize=chunksize,
            names=list(config.process.use_columns),
            nrows=nrows,
        )

    df_open = open_hdf5_data(file=hdf5_file, nrows=nrows)
    print(f'Number of rows has been opened "{hdf5_file}": {len(df_open)}')

    df_ppd, _ = add_join_column(
        df_open
    )  # Add concatenate column which consist data from all group columns

    dh = process_grouping_data(
        df=df_ppd, groupby=list(config.process.use_group)
    )
    df = process_joining_data(
        df=df_ppd, dh=dh, groupby=list(config.process.use_group)
    )

    process_export_data(
        df=df, file=out_file, chunksize=int(config.final.chunksize)
    )


if __name__ == "__main__":
    process_data()
