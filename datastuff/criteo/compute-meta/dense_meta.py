import json
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

import boto3
from pyspark.sql import SparkSession  # type: ignore
import click


@click.command()
@click.option("--data-url", help="S3 location of the parquet files.")
@click.option("--metadata-url", help="S3 location of dense.json.")
def main(data_url, metadata_url):
    spark = SparkSession.builder.getOrCreate()
    intcols = [f"i{i}" for i in range(1, 14)]
    df = spark.read.parquet(data_url)
    desc = df.select(*intcols).describe()
    desc_df = desc.toPandas()
    stats = {}
    for col in intcols:
        stats[col] = {
            "count": None,
            "mean": None,
            "stddev": None,
            "min": None,
            "max": None,
        }
    for _, row in desc_df.iterrows():
        for col in intcols:
            stats[col][row["summary"]] = float(row[col])

    with TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname)
        mlfile = tmpdir / "dense.json"
        with mlfile.open("wt") as jf:
            json.dump(stats, jf, indent=2)

        s3 = boto3.client("s3")
        s3url = urlparse(metadata_url)
        if s3url.scheme != "s3":
            raise RuntimeError("Metadata url has to be an s3 url!")
        bucket = s3url.netloc
        prefix = str(Path(s3url.path).relative_to("/"))
        print(f"Uploading to {bucket}/{prefix}")
        s3.upload_file(str(mlfile), bucket, prefix)


if __name__ == "__main__":
    main()
