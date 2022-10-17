"""Extract-Transform-Load utility for Criteo dataset.

This stand-alone module is used to download a single file from the Criteo dataset,
transform it from gzipped csv to parquet, and then upload it back to a specified
S3 location.

Attributes:
    CHUNK_SZ_BYTES: The number of bytes to download from Criteo in one go.

Example::

    python etl.py --criteo-url='https://storage.googleapis.com/criteo-cail-datasets/day_0.gz' --s3-url='s3://avilabs-mldata-us-west-2/criteo/onetb/data/day=0/'

"""
import shutil
from collections.abc import Iterator
from itertools import chain
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional
from urllib.parse import urlparse

import boto3
import click
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.dataset as ds
import requests
import shell
from botocore.exceptions import ClientError
from cprint import danger_print, info_print, warning_print
from pyarrow.lib import ArrowInvalid
from tqdm import tqdm

CHUNK_SZ_BYTES = 1024


def download(criteo_url: str, tmpdir: Path) -> Path:
    info_print("Downloading criteo archive.")

    url = urlparse(criteo_url).path
    file = tmpdir / Path(url).name
    r = requests.get(criteo_url, stream=True)
    total_bytes = int(r.headers.get("content-length", 0))
    progress_bar = tqdm(total=total_bytes, unit="iB", unit_scale=True)
    with open(file, "wb") as f:
        for chunk in r.iter_content(chunk_size=CHUNK_SZ_BYTES):
            progress_bar.update(len(chunk))
            f.write(chunk)
    progress_bar.close()
    return file


def unpack(archive: Path) -> Iterator[Path]:
    info_print(f"Unpacking downloaded archive {archive.name}.")

    tmpdir = archive.parent
    shell.enable_gzip()
    shutil.unpack_archive(filename=archive, extract_dir=tmpdir)
    archive.unlink()
    return tmpdir.iterdir()


def totsv(file: Path) -> Optional[ds.Dataset]:
    """Creates a schematized PyArrow TSV dataset.

    Loads the input file as a PyArrow TSV dataset with the following schema -
      - The first 13 columns as integers and named i1, i2, ..., i13
      - The next 26 columns as strings and named s1, s2, ..., s26
      - The last column as an integer and named label

    Files that are not TSV files or those that do not have the right schema are
    ignored.

    Args:
        file: The local path of the file to be convered.

    Returns:
        A PyArrow dataset object representing the schematized TSV or None if the
        file was not a TSV or did not have the right schema.
    """
    info_print(f"Converting {file.name} to TSV dataset.")

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
    dataset = ds.dataset(source=file, schema=schema, format=format)

    # Check that this is a TSV file in the expected format
    try:
        dataset.head(5)
        return dataset
    except ArrowInvalid:
        warning_print(f"\t{file} is not a TSV file. Skipping.")
        return None


def topq(tsv: ds.Dataset) -> Iterator[Path]:
    """Converts a TSV dataset to one or more partitioned Parquet files.

    Takes the input TSV dataset and creates Parquet files in the same directory
    that had the original TSV file. This function uses the default dataset writer
    so it will result in only a single

    Args:
        tsv: Schematized PyArrow TSV dataset.

    Returns:
        Local paths of one or more partitioned Parquet file.
    """
    info_print("Converting TSV dataset to Parquet file.")

    source_file = Path(tsv.files[0])
    wo = ds.ParquetFileFormat().make_write_options(compression="gzip")
    ds.write_dataset(
        tsv,
        source_file.parent,
        format="parquet",
        file_options=wo,
        basename_template=f"{source_file.stem}_" + "{i}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )

    return source_file.parent.glob("*.parquet")


def upload(s3_url: str, pq: Path) -> None:
    """Uploads the Parquet file to the S3 URL.

    Creates an S3 key by appending the name of the Parquet file
    (e.g., filename.ext) to the passed in S3 prefix (e.g., /path/to/folder/) so
    that the resulting key is /path/to/folder/filename.ext. Uploads the Parquet
    file with this key to S3.

    Args:
        s3_url: This is in the form of s3://bucket/path/to/folder/.
        pq: This is the full local path of the Parquet file.

    Returns:
        None: Regardless of whether the file upload was successful or not, this
        function will always return None. For the unsuccessful case, it will
        print the error before returning.
    """
    info_print(f"Uploading {pq.name} to S3.")

    total_bytes = pq.stat().st_size
    s3url = urlparse(s3_url)
    bucket = s3url.netloc
    prefix = Path(s3url.path).relative_to("/")
    key = prefix / pq.name
    progress_bar = tqdm(total=total_bytes, unit="iB", unit_scale=True)
    try:
        s3 = boto3.client("s3")
        s3.upload_file(
            str(pq), bucket, str(key), Callback=lambda chunk: progress_bar.update(chunk)
        )
    except ClientError as err:
        danger_print("Unable to upload!")
        print(err)


@click.command(help=__doc__)
@click.option(
    "--criteo-url",
    help="The source url to download the data archive file from.",
)
@click.option(
    "--s3-url",
    help="The destination S3 location to upload the parquet files to.",
)
def main(criteo_url: str, s3_url) -> None:
    with TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname)
        archive = download(criteo_url, tmpdir)
        files = unpack(archive)
        tsvs = filter(lambda x: x is not None, map(totsv, files))
        pqs = chain.from_iterable(map(topq, tsvs))
        for pq in pqs:
            upload(s3_url, pq)


if __name__ == "__main__":
    main()
