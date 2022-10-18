import json
from pathlib import Path
from urllib.parse import urlparse
from tempfile import TemporaryDirectory

import boto3
from pyspark.sql import SparkSession  # type: ignore
import click


@click.command()
@click.option("--data-url", required=True, help="S3 location of the parquet files.")
@click.option(
    "--metadata-url", required=True, help="S3 location where metadata will be saved."
)
def main(data_url, metadata_url):
    spark = SparkSession.builder.getOrCreate()
    strcols = [f"s{i}" for i in range(1, 27)]
    df = spark.read.parquet(data_url)

    all_toks = {}
    for col in strcols:
        all_toks[col] = {}
        toks = df.groupby(col).count().sort("count", ascending=False).collect()
        for tok, count in toks:
            all_toks[col][tok] = count

    with TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname)
        mlfile = tmpdir / "sparse.json"
        with mlfile.open("wt") as jf:
            json.dump(all_toks, jf, indent=2)

        s3 = boto3.client("s3")
        s3url = urlparse(metadata_url)
        if s3url.scheme != "s3":
            raise RuntimeError("Metadata url has to be an S3 url!")
        bucket = s3url.netloc
        prefix = str(Path(s3url.path).relative_to("/"))
        print(f"Uploading to {bucket}/{prefix}")
        s3.upload_file(str(mlfile), bucket, prefix)


if __name__ == "__main__":
    main()
