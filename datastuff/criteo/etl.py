"""Extract-Transform-Load utility for Criteo dataset.

This stand-alone module is used to download a single file from the Criteo 
dataset, transform it from gzipped csv to gzipped parquet, and then upload it 
back to a specified S3 location.

Attributes:
    CHUNK_SZ_BYTES: The number of bytes to download from Criteo in one go.

Example::

    python etl.py --criteo-url='https://storage.googleapis.com/criteo-cail-datasets/day_0.gz' --s3-url='s3://avilabs-mldata-us-west-2/criteo/onetb/data/day=0/'

"""
import shutil
from collections.abc import Iterator

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional
from urllib.parse import urlparse

import boto3
import click
import msg
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
    """Downloads the Criteo data file.

    The Criteo URL is usually in the form https://path/to/file/filename.tar.gz.
    This function will download the file as `tmpdir/filename.tar.gz`. It also
    shows a helpful little progress bar for the download.

    Args:
        criteo_url: The URL from where to download the file.
        tmpdir: The directory where the downloaded file is to be saved.

    Returns:
        The local path of the downloaded file.
    """
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
    """Unpacks the input archive file.

    The input archive file is unpacked in the same directory it is in. The directory
    is expected to have only the single archive file in it. The archive file
    is expected to be in .tar.gz format and can have multiple files packed in it.
    The archive file is deleted after it has been unpacked, and all the remaining
    unpacked files in the directory are returned as an iterator.

    Args:
        archive: The local path of the archive file.

    Returns:
        An iterator of all the unpacked files.
    """
    info_print(f"Unpacking downloaded archive {archive.name}.")

    tmpdir = archive.parent
    if len(list(tmpdir.iterdir())) > 1:
        raise RuntimeError(f"{tmpdir} must have only the downloaded file in it!")
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


def topq(tsv: ds.Dataset) -> Path:
    """Converts TSV dataset into Parquet format.

    Creates a gzipped Parquet file in the same directory as the input TSV file. If the
    input TSV file is in /path/to/filename.tsv then the Parquet file will be
    saved as /path/to/filename_0.parquet.

    Args:
        tsv: The PyArrow Dataset representing the original TSV file.

    Returns:
        The local file path of the output Parquet file.
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

    expected_outfile = source_file.parent / f"{source_file.stem}_0.parquet"
    if not expected_outfile.exists():
        raise RuntimeError(
            f"Expected to see {expected_outfile} but instead found {list(source_file.parent.glob('*.parquet'))}"
        )
    return expected_outfile


def upload(s3_url: str, pq: Path) -> None:
    """Uploads the Parquet file to the S3 URL.

    Creates an S3 key by appending the name of the Parquet file
    (e.g., filename.ext) to the passed in S3 prefix (e.g., /path/to/folder/) so
    that the resulting key is /path/to/folder/filename.ext. Uploads the Parquet
    file with this key to S3.

    Args:
        s3_url: This is in the form of s3://bucket/path/to/folder/
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
        pqs = map(topq, tsvs)
        for pq in pqs:
            upload(s3_url, pq)
    msg.sms("+12066173488", f"Processed {criteo_url[:50]}")


if __name__ == "__main__":
    main()
