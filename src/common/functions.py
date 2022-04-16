"""
Common function use in project

author: Dmitriy Grigorev
"""

import os
import time
from pathlib import Path
from urllib.parse import urlencode

import requests
import vaex
from dotenv import load_dotenv
from hydra.utils import to_absolute_path as abspath
from omegaconf import DictConfig
from tqdm import tqdm

load_dotenv(dotenv_path=Path("common/.env"), override=True)

DEBUG = os.getenv("DEBUG") == "True"
BASE_DIR = str(Path(__file__).parent.parent.parent) + os.path.sep


def raw_file_name(config: DictConfig):
    """Return absolute path to the source CSV file"""
    file = abspath(BASE_DIR + config.processed.path)
    if not os.path.isfile(file):
        download_from_ya_disk(urls=config.process.use_urls, file_path=file)
    return file


def hdf5_file_name(config: DictConfig):
    """Return absolute path to the HDF5 file"""
    return abspath(BASE_DIR + config.processed.hdf5)


def out_file_name(config: DictConfig):
    """Return absolute path to the output CSV file"""
    return abspath(BASE_DIR + config.final.path)


def empty_dataframe(config: DictConfig) -> vaex.dataframe.DataFrame:
    """Return empty vaex.dataframe.DataFrame based on the source file"""
    return vaex.read_csv(raw_file_name(config), nrows=0)


def calculate_elapsed_time(start_time) -> str:
    """Calculate elapsed time"""
    elapsed_time = time.time() - start_time
    return f"{round(elapsed_time,2)}s = {round(elapsed_time/60,1)}m = {round((elapsed_time/60) / 60,1)}h"


def concat_cols(dt, cols, name="concat_col", divider="|"):
    """Create the join column"""
    for i, col in enumerate(cols):
        if i == 0:
            dt[name] = dt[col].astype("string").fillna("")
        else:
            dt[name] = dt[name] + divider + dt[col].astype("string").fillna("")

    # Ensure it's a string; on rare occassions it's an object
    dt[name] = dt[name].astype("string")

    return dt, name


def add_join_column(df, name="concat_col", divider="|"):
    """Create the join column"""
    df[name] = (
        df["paon"]
        + divider
        + df["street"]
        + divider
        + df["locality"]
        + divider
        + df["towncity"]
        + divider
        + df["district"]
        + divider
        + df["country"]
    )
    return df, name


def download_from_ya_disk(urls, file_path) -> None:
    """Download source file from yandex disk storage"""
    base_url = urls[0]
    public_key = urls[1]
    final_url = base_url + urlencode(dict(public_key=public_key))
    response = requests.get(final_url)
    download_url = response.json()["href"]

    r = requests.get(download_url, stream=True)
    if r.ok:
        total_size_in_bytes = int(response.headers.get("content-length", 0))
        block_size = 1024  # 1 Kibibyte
        print(f"Download and save data file to the: {file_path}")
        progress_bar = tqdm(
            total=total_size_in_bytes,
            unit="iB",
            unit_scale=True,
            desc="download",
        )
        with open(file_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=block_size * 8):
                if chunk:
                    f.write(chunk)
                    progress_bar.update(len(chunk))
                    f.flush()
                    os.fsync(f.fileno())
        progress_bar.close()
        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            print(
                f"Download failed: file {file_path} is not correctly saved."
                f"Please delete and restart processing."
            )
    else:  # HTTP status code 4XX/5XX
        print(f"Download failed: status code {r.status_code}\n{r.text}")
