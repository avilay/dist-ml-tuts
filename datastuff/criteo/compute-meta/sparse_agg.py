"""Aggregate the statistics from individual metadata files.

This module will take the sparse statistics of a single column from multiple
days and aggregate them into a single CSV file.

Inputs:
    s3://avilabs-mldata-us-west-2/criteo/onetb/metadata/day=0/s1.csv
    s3://avilabs-mldata-us-west-2/criteo/onetb/metadata/day=1/s1.csv
    :
    s3://avilabs-mldata-us-west-2/criteo/onetb/metadata/day=30/s1.csv

Output:
    s3://avilabs-mldata-us-west-2/criteo/onetb/metadata/sparse/s1.csv
"""
