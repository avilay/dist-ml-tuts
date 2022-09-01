import os
import shutil
import sys
from pathlib import Path
from typing import Optional, cast
from urllib.parse import urlparse

import boto3
import click
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.dataset as ds
import requests
from botocore.exceptions import ClientError
from cprint import danger_print, warning_print
from pyarrow.lib import ArrowInvalid
from tqdm import tqdm
from glob import glob
import gzip

CHUNK_SZ_BYTES = 1024


# TODO: Add these functions to utils
def print_now(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()


def gunzip(src_file: str, dst_dir: str) -> None:
    dst_file: Path = Path(dst_dir) / Path(src_file).stem
    with gzip.open(src_file, "rb") as fsrc:
        with open(dst_file, "wb") as fdst:
            shutil.copyfileobj(fsrc, fdst)


registered_exts = [ext for _, exts, _ in shutil.get_unpack_formats() for ext in exts]
if ".gz" not in registered_exts:
    shutil.register_unpack_format("gunzip", [".gz"], gunzip)


# The main code of this file
def download(dataroot: Path, download_url: Optional[str]) -> list[Path]:
    print("---")

    tsvroot = dataroot / "tsv"
    os.makedirs(tsvroot, exist_ok=True)
    urlpath: str = cast(str, urlparse(download_url).path) if download_url else ""
    filename = Path(urlpath).name
    dst = tsvroot / filename

    print(f"Downloading {download_url} to {dst}.")

    if not dst.exists() and download_url:
        # Download the file, mostly this will be some sort of archive file
        r = requests.get(download_url, stream=True)
        total_bytes = int(r.headers.get("content-length", 0))
        progress_bar = tqdm(total=total_bytes, unit="iB", unit_scale=True)
        with open(dst, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024):
                progress_bar.update(len(chunk))
                f.write(chunk)
        progress_bar.close()

        # Extract the downloaded archive, this can result in multiple files
        shutil.unpack_archive(filename=dst, extract_dir=tsvroot)
        os.remove(dst)
    else:
        warning_print("Nothing to download.")

    return [tsvroot / fname for fname in os.listdir(tsvroot)]


def load_tsv(tsvfile: Path) -> Optional[ds.Dataset]:
    print("---")

    print(f"Loading TSV {tsvfile} into dataset.")
    if not tsvfile.is_file():
        warning_print("No TSV to load. Skipping.")
        return None

    intcols = [f"i{i}" for i in range(1, 14)]
    strcols = [f"s{i}" for i in range(1, 27)]
    colnames = ["label"] + intcols + strcols

    labelschema = [("label", pa.int8())]
    intschema = [(colname, pa.int32()) for colname in intcols]
    strschema = [(colname, pa.string()) for colname in strcols]
    schema = pa.schema(labelschema + intschema + strschema)

    format = ds.CsvFileFormat(
        parse_options=pcsv.ParseOptions(delimiter="\t"),
        read_options=pcsv.ReadOptions(column_names=colnames),
    )

    dataset = ds.dataset(source=tsvfile, schema=schema, format=format)

    # Check that this is a TSV file in the expected format
    try:
        dataset.head(5)
        return dataset
    except ArrowInvalid:
        warning_print(f"{tsvfile} is not a TSV file. Skipping.")
        return None


def write_parquet(
    dataroot: Path, dataset: Optional[ds.Dataset], filepath: Path
) -> list[Path]:
    print("---")

    print("Writing dataset to parquet.")
    filename = filepath.stem
    pqroot = dataroot / "parquet"
    os.makedirs(pqroot, exist_ok=True)

    if dataset:
        wo = ds.ParquetFileFormat().make_write_options(compression="gzip")
        ds.write_dataset(
            dataset,
            pqroot,
            format="parquet",
            file_options=wo,
            basename_template=f"{filename}_" + "{i}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )
    else:
        warning_print("Nothing to write as Parquet. Skipping.")

    return [Path(f) for f in glob(str(pqroot / f"{filename}_*.parquet"))]


def upload_to_s3(src: Path, dst_s3url: str) -> None:
    print("---")

    print(f"Uploading {src} to {dst_s3url}")
    total_bytes = src.stat().st_size
    s3url = urlparse(dst_s3url)
    bucket = s3url.netloc
    prefix = Path(s3url.path).relative_to("/")
    key = prefix / src.name
    progress_bar = tqdm(total=total_bytes, unit="iB", unit_scale=True)
    try:
        s3 = boto3.client("s3")
        s3.upload_file(
            str(src),
            bucket,
            str(key),
            Callback=lambda chunk: progress_bar.update(chunk),
        )
        os.remove(src)
    except ClientError as err:
        danger_print(f"Unable to upload {src} to {dst_s3url}!")
        print(err)


@click.command()
@click.option("--dataroot", type=click.Path(), default=".", help="Local data root.")
@click.option(
    "--s3url",
    required=True,
    help="The destination S3 URL s3://<bucket>/<prefix>/<filename>.<ext>.",
)
@click.option(
    "--download-url",
    help="The source url to download the tsv file from.",
)
def main(dataroot: str, s3url: str, download_url: Optional[str]) -> None:
    """
    \b
    Tool to convert criteo CSV files to gzipped parquet files and upload them to S3.
      * Download csv file to local disk
      * Read csv file from disk to pyarrow dataset
      * Write pyarrow dataset as a parquet file to disk
      * Upload parquet file to S3
    \b
    Example:
    prepare.py
    --dataroot='/home/admin/mldata/criteo/kaggle'
    --s3url='s3://avilabs-mldata-us-west-2/temp'
    --download-url='https://go.criteo.net/criteo-research-kaggle-display-advertising-challenge-dataset.tar.gz'
    """

    dataroot_path = Path(dataroot)
    downloaded_files = download(dataroot_path, download_url)
    for downloaded_file in downloaded_files:
        dataset = load_tsv(downloaded_file)
        files_to_upload = write_parquet(dataroot_path, dataset, downloaded_file)
        for file_to_upload in files_to_upload:
            upload_to_s3(file_to_upload, s3url)
        os.remove(downloaded_file)


if __name__ == "__main__":
    main()
