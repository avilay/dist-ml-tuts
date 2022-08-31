import os
import shutil
import sys
from pathlib import Path
from typing import Optional
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
def download(dataroot, download_url: str) -> list[Path]:
    print(f"\nDownloading {download_url} to {dataroot}.")
    filename = Path(urlparse(download_url).path).name
    tsvroot = dataroot / "tsv"
    os.makedirs(tsvroot, exist_ok=True)
    dst = tsvroot / filename

    if not dst.exists():
        r = requests.get(download_url, stream=True)
        total_bytes = int(r.headers.get("content-length", 0))
        progress_bar = tqdm(total=total_bytes, unit="iB", unit_scale=True)
        with open(dst, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024):
                progress_bar.update(len(chunk))
                f.write(chunk)
        progress_bar.close()
    else:
        warning_print(f"{dst} already exists, skipping downloading.")

    try:
        print_now("Extracting...", end="")
        shutil.unpack_archive(filename=dst, extract_dir=tsvroot)
        print_now("Complete.")

        # print_now("Deleting archive...", end="")
        # os.remove(dst)
        # print_now("Complete.")

        return [tsvroot / fname for fname in os.listdir(tsvroot)]
    except shutil.ReadError:
        danger_print(f"Unable to extract {dst}")
        raise


def load_tsv(tsvfile: Path) -> Optional[ds.Dataset]:
    print(f"\nLoading {tsvfile} as an Arrow dataset.")
    intcols = [f"i{i}" for i in range(1, 14)]
    strcols = [f"s{i}" for i in range(1, 27)]
    colnames = ["label"] + intcols + strcols

    labelschema = [("label", pa.int8())]
    intschema = [(colname, pa.int32()) for colname in intcols]
    strschema = [(colname, pa.string()) for colname in strcols]
    schema = pa.schema(labelschema + intschema + strschema)

    print(f"Loading {tsvfile} into dataset.")
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


def write_parquet(dataroot: Path, dataset: ds.Dataset, filename: str) -> list[Path]:
    print()
    pqroot = dataroot / "parquet"
    os.makedirs(pqroot, exist_ok=True)
    wo = ds.ParquetFileFormat().make_write_options(compression="gzip")
    print_now(f"Writing dataset as parquet to {pqroot}...", end="")
    ds.write_dataset(
        dataset,
        pqroot,
        format="parquet",
        file_options=wo,
        basename_template=f"{filename}_" + "{i}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )
    print_now("Complete.")
    return [Path(f) for f in glob(str(pqroot / f"{filename}_*.parquet"))]


def upload_to_s3(src: Path, dst_s3url: str) -> None:
    print()
    total_bytes = src.stat().st_size

    s3url = urlparse(dst_s3url)
    bucket = s3url.netloc
    prefix = Path(s3url.path).relative_to("/")
    key = prefix / src.name
    print(f"Uploading {src} to s3://{bucket}/{key}")
    progress_bar = tqdm(total=total_bytes, unit="iB", unit_scale=True)
    try:
        s3 = boto3.client("s3")
        s3.upload_file(
            str(src),
            bucket,
            str(key),
            Callback=lambda chunk: progress_bar.update(chunk),
        )
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
    dataroot_path: Path = Path(dataroot)
    if download_url:
        downloaded_files: list[Path] = download(dataroot_path, download_url)
    else:
        tsvroot = dataroot_path / "tsv"
        downloaded_files = [tsvroot / fname for fname in os.listdir(tsvroot)]
    for downloaded_file in downloaded_files:
        dataset: Optional[ds.Dataset] = load_tsv(downloaded_file)
        files_to_upload: list[Path] = (
            write_parquet(dataroot_path, dataset, downloaded_file.stem)
            if dataset
            else [downloaded_file]
        )
        for file_to_upload in files_to_upload:
            upload_to_s3(file_to_upload, s3url)


if __name__ == "__main__":
    main()
