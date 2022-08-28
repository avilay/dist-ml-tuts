from pathlib import Path
from urllib.parse import urlparse

import boto3
import click
import pyarrow as pa
import requests
from botocore.exceptions import ClientError
from cprint import danger_print, info_print, warning_print
from tqdm import tqdm

DATAROOT = Path.home() / "temp/criteo-kaggle-small/data"
CHUNK_SZ_BYTES = 1024


def download(download_url: str) -> Path:
    filename = Path(urlparse(download_url).path).stem
    dst = DATAROOT / "tsv" / filename
    if dst.exists():
        warning_print(f"{dst} already exists, skipping downloading.")
        return dst

    r = requests.get(download_url, stream=True)
    total_bytes = int(r.headers.get("content-length", 0))
    progress_bar = tqdm(total=total_bytes, unit="iB", unit_scale=True)
    with open(dst, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024):
            progress_bar.update(len(chunk))
            f.write(chunk)

    progress_bar.close()

    return dst


def load_tsv(tsvfile: Path) -> pa.dataset.Dataset:
    intcols = [f"i{i}" for i in range(1, 14)]
    strcols = [f"s{i}" for i in range(1, 27)]
    colnames = ["label"] + intcols + strcols

    labelschema = [("label", pa.int8())]
    intschema = [(colname, pa.int32()) for colname in intcols]
    strschema = [(colname, pa.string()) for colname in strcols]
    schema = pa.schema(labelschema + intschema + strschema)

    info_print(f"Loading {tsvfile} into dataset.")
    format = pa.dataset.CsvFileFormat(
        parse_options=pa.csv.ParseOptions(delimiter="\t"),
        read_options=pa.csv.ReadOptions(column_names=colnames),
    )

    return pa.dataset.dataset(source=tsvfile, schema=schema, format=format)


def write_parquet(ds: pa.dataset.Dataset, filename: str) -> Path:
    dst = DATAROOT / "parquet" / filename
    wo = pa.dataset.ParquetFileFormat().make_write_options(compression="gzip")
    info_print(f"Writing dataset to {filename}.")
    ds.write_dataset(ds, dst, format="parquet", file_options=wo)
    return dst


def upload_to_s3(src: Path, dst_s3url: str) -> None:
    s3 = boto3.client("s3")
    s3url = urlparse(dst_s3url)
    bucket = s3url.netloc
    prefix = Path(s3url.path)
    objname = prefix / src.name
    try:
        info_print(f"Uploading {src} to {dst_s3url}.")
        resp = s3.upload_file(src, bucket, objname)
    except ClientError as e:
        danger_print(f"Unable to upload {src} to {dst_s3url}!")
        danger_print(e)
        print(resp)


@click.command()
@click.argument("download_url")
@click.argument("upload_s3_url")
def main(download_url, upload_s3_url):
    """
    Tool to convert criteo CSV files to gzipped parquet files and upload them to S3.
      * Download csv file to local disk
      * Read csv file from disk to pyarrow dataset
      * Write pyarrow dataset as a parquet file to disk
      * Upload parquet file to S3
    """
    tsvpath: Path = download(download_url)
    ds: pa.dataset.Dataset = load_tsv(tsvpath)
    pqpath: Path = write_parquet(ds, tsvpath.name)
    upload_to_s3(pqpath, upload_s3_url)


if __name__ == "__main__":
    main()
