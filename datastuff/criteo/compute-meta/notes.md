## EMR Serverless

#### Script Location
s3://avilabs-us-west-2-scratch/scripts/dense_meta.py

```
aws s3 cp dense_meta.py s3://avilabs-us-west-2-scratch/scripts/dense_meta.py
```

```
aws s3 cp sparse_meta.py s3://avilabs-us-west-2-scratch/scripts/sparse_meta.py
```

```
aws s3 cp s3://avilabs-us-west-2-scratch/logs/applications/00f4s1c7rdsmgm0l/jobs/00f4smtf93q7050l/ . --recursive
```

```
create external table if not exists clickstream (
    i1 int,
    i2 int,
    i3 int,
    i4 int,
    i5 int,
    i6 int,
    i7 int,
    i8 int,
    i9 int,
    i10 int,
    i11 int,
    i12 int,
    i13 int,
    s1 string,
    s2 string,
    s3 string,
    s4 string,
    s5 string,
    s6 string,
    s7 string,
    s8 string,
    s9 string,
    s10 string,
    s11 string,
    s12 string,
    s13 string,
    s14 string,
    s15 string,
    s16 string,
    s17 string,
    s18 string,
    s19 string,
    s21 string,
    s22 string,
    s23 string,
    s24 string,
    s25 string,
    s26 string,
    label int
)
PARTITIONED BY (day STRING)
STORED AS PARQUET
LOCATION 's3://avilabs-mldata-us-west-2/criteo/onetb/data/'
tblproperties ("parquet.compression"="GZIP");
```


#### Script Arguments: 
  * --data-url=s3://avilabs-mldata-us-west-2/criteo/onetb/data/
  * --metadata-url=s3://avilabs-us-west-2-scratch/output/dense2.json

#### Spark Properties

  * --conf spark.executor.cores=2 
  * --conf spark.executor.memory=8g 
  * --conf spark.driver.cores=2 
  * --conf spark.driver.memory=8g 
  * --conf spark.executor.instances=2 
  * --conf spark.archives=s3://avilabs-us-west-2-scratch/venvs/sparkenv.tar.gz#environment 
  * --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python 
  * --conf park.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python 
  * --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python 
  * --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory

The last config in the list is set automatically.

#### Additional Settings
Check the box for **Upload logs to your Amazon S3 bucket** and set the bucket location to `s3://avilabs-us-west-2-scratch/logs/`.
