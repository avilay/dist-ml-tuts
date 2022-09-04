import click
from pathlib import Path
from typing import Optional
from collections.abc import Iterator
import pyarrow.dataset as ds

# from tempfile import TemporaryDirectory
from urllib.parse import urlparse
import requests
from tqdm import tqdm
import shell
import shutil
from cprint import info_print, danger_print
import pyarrow as pa
import pyarrow.csv as pcsv
from pyarrow.lib import ArrowInvalid
from itertools import chain
import boto3
from botocore.exceptions import ClientError


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
        info_print(f"{file} is not a TSV file. Skipping.")
        return None


def topq(tsv: ds.Dataset) -> Iterator[Path]:
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


@click.command()
@click.option(
    "--criteo-url",
    help="The source url to download the data archive file from.",
)
@click.option(
    "--s3-url",
    help="The destination S3 location to upload the parquet files to.",
)
def main(criteo_url: str, s3_url) -> None:
    """Help for this script. Here is where I explain what arg is doing."""
    # with TemporaryDirectory() as tmpdirname:
    # tmpdir = Path(tmpdirname)
    tmpdir = Path.home() / "temp" / "prepare"
    archive = download(criteo_url, tmpdir)
    files = unpack(archive)
    tsvs = filter(lambda x: x is not None, map(totsv, files))
    pqs = chain.from_iterable(map(topq, tsvs))
    for pq in pqs:
        upload(s3_url, pq)


if __name__ == "__main__":
    main()
