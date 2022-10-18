## EMR Serverless

#### Script Location
s3://avilabs-us-west-2-scratch/scripts/dense_meta.py

```
aws s3 cp dense_meta.py s3://avilabs-us-west-2-scratch/scripts/dense_meta.py
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
