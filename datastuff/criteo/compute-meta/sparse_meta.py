"""Compute statistics of sparse columns.

This module will compute the statistics of each of the sparse columns in a
single data.parquet file. It will store the computed statistics in another
file on S3.

Input: s3://avilabs-mldata-us-west-2/criteo/onetb/data/day=0/data.parquet
Outputs:
    s3://avilabs-mldata-us-west-2/criteo/onetb/metadata/day=0/s1.csv
    s3://avilabs-mldata-us-west-2/criteo/onetb/metadata/day=0/s2.csv
    :
    s3://avilabs-mldata-us-west-2/criteo/onetb/metadata/day=0/s26.csv

The sparse statistics file will look like this -

s2,0
00007246,901
00032064,100195
0006570e,722
0006a0b0,29
000877b3,154482
000bf672,4774
000eaaf3,354
0010008d,15
00105b58,294
::

"""
import click
from tempfile import TemporaryDirectory
from pathlib import Path
import pandas as pd


def download(data_file: str, tmpdir: Path) -> Path:
    pass


def compute_stats(pqfile: Path, colnam: str) -> pd.DataFrame:
    pass


def save_stats(tmpdir: Path, df: pd.DataFrame) -> Path:
    pass


@click.command(help=__doc__)
@click.option(
    "--data-file",
    help="S3 url of the data.parquet file.",
)
@click.option(
    "--metadata-folder", help="S3 url where the computed metadata will be uploaded."
)
def main(data_file: str, metadata_folder: str) -> None:
    with TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname)
        pqfile = download(data_file, tmpdir)
        for s in sparse_cols:
            df = compute_stats(pqfile, s)
            csv = save_stats(tmpdir, df)

            upload(outfile, csv)
    msg.sms("+12066173488", f"Processed {day}")


if __name__ == "__main__":
    main()
