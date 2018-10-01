"""Register Adobe Analytics Data Feed files in Athena"""
import os
import time
from io import BytesIO
from gzip import GzipFile
from urllib.parse import urlparse

import boto3


def get_s3_file(s3_uri):
    """Retrieve an S3 file"""
    s3_client = boto3.client('s3')
    url_object = urlparse(s3_uri)
    bucket = url_object.netloc
    key = url_object.path.lstrip('/')

    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response['Body']


def create_db(glue_client, database_name):
    """Create the specified Glue database if it does not exist"""
    try:
        glue_client.get_database(Name=database_name)
    except glue_client.exceptions.EntityNotFoundException:
        print("Creating database: %s" % database_name)
        glue_client.create_database(
            DatabaseInput={'Name': database_name}
        )


def get_headers(s3_report_base, partition_date):
    """Retrieve the Adobe Analytics Data Feed column headers file from S3 and return it as a Python list"""
    header_file = os.path.join(s3_report_base, "rawtsv", "column_headers", "dt=%s" % partition_date, "column_headers.tsv.gz")
    bytestream = BytesIO(get_s3_file(header_file).read())
    got_text = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
    return got_text.strip().split("\t")


def create_glue_table(glue_client, database_name, s3_report_base, partition_date):
    """Create the base Glue table if it does not exist"""
    if does_table_exist(glue_client, database_name, "hit_data"):
        return
    print("Creating hit_data table using partition %s" % partition_date)
    headers = get_headers(s3_report_base, partition_date)
    glue_client.create_table(
        DatabaseName=database_name,
        TableInput={
            "Name": 'hit_data',
            "StorageDescriptor": storage_descriptor(
                [{"Type": "string", "Name": name} for name in headers],
                '%s/rawtsv/hit_data/' % (s3_report_base)
            ),
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {},  # Required or Glue create_dynamic_frame.from_catalog fails
            "LastAccessTime": time.time()
        }
    )


def does_partition_exist(glue_client, database, table, part_values):
    """Test if a specific partition exists"""
    try:
        glue_client.get_partition(DatabaseName=database, TableName=table, PartitionValues=part_values)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False


def add_hitdata_partition(glue_client, database_name, s3_report_base, partition_date):
    """Add a partition to the hit_data table for a specific date"""
    if does_partition_exist(glue_client, database_name, "hit_data", [partition_date]):
        return
    headers = get_headers(s3_report_base, partition_date)
    print("Creating partition %s" % partition_date)
    glue_client.create_partition(
        DatabaseName=database_name,
        TableName="hit_data",
        PartitionInput={
            "Values": [partition_date],
            "StorageDescriptor": storage_descriptor(
                [{"Type": "string", "Name": name} for name in headers],
                '%s/rawtsv/hit_data/dt=%s/' % (s3_report_base, partition_date)
            ),
            "Parameters": {}
        }
    )


def storage_descriptor(columns, location):
    """Dynamically build a Data Catalog storage descriptor with the desired columns and S3 location"""
    return {
        "Columns": columns,
        "Location": location,
        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
            "Parameters": {
                "separatorChar": "\t"
            }
        },
        "BucketColumns": [],  # Required or SHOW CREATE TABLE fails
        "Parameters": {}  # Required or create_dynamic_frame.from_catalog fails
    }


def does_table_exist(glue_client, database, table):
    """Test if a specific table exists"""
    try:
        glue_client.get_table(DatabaseName=database, Name=table)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False


def create_lookup_tables(glue_client, database_name, s3_lookup_uri):
    """Create all the Data Catalog tables for Adobe Analytics lookup tables"""
    generic_lookup_types = [
        'browser',
        'browser_type',
        'color_depth',
        'connection_type',
        'country',
        'javascript_version',
        'languages',
        'operating_systems',
        'plugins',
        'resolution',
        'referrer_type',
        'search_engines',
    ]

    for lookup_name in generic_lookup_types:
        if does_table_exist(glue_client, database_name, lookup_name):
            continue
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                "Name": lookup_name,
                "StorageDescriptor": storage_descriptor(
                    [{"Type": "string", "Name": "id"}, {"Type": "string", "Name": "value"}],
                    '%s/%s/' % (s3_lookup_uri, lookup_name)
                ),
                "PartitionKeys": [],
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {},  # Required or Glue create_dynamic_frame.from_catalog fails
                "LastAccessTime": time.time()
            }
        )


def create_glue_client():
    """Create a return a Glue client for the region this AWS lambda job is running in"""
    current_region = os.environ['AWS_REGION']  # With Glue, we only support writing to the region where this code runs
    return boto3.client('glue', region_name=current_region)


def handler(event, context):
    """AWS Lambda event handler for maintaining Glue Data Catalog metadata.

    The calling function will provide the Adobe Analytics report date, the base S3 URI where convert report data is
    stored, as well as the location of the latest lookup files.

    This script will create the requisite Glue Data Catalog database specified in the CloudFormation template, hit_data
    and lookup tables, and add a partition for the provided hit_data date if it does not yet exist.
    """
    print("Got event", event)
    s3_report_base = event['reportBase'].rstrip('/')
    lookup_location = event['lookupURI'].rstrip('/')
    partition_date = event['reportDate']
    glue_database = os.environ['DB_NAME']
    glue_client = create_glue_client()

    create_db(glue_client, glue_database)
    create_glue_table(glue_client, glue_database, s3_report_base, partition_date)
    create_lookup_tables(glue_client, glue_database, lookup_location)
    add_hitdata_partition(glue_client, glue_database, s3_report_base, partition_date)
