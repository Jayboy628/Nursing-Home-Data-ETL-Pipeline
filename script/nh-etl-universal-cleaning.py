import sys
import boto3
import os
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, monotonically_increasing_id, current_date
from pyspark.sql.types import LongType, DateType

# Parse job arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'SOURCE_PATH', 'STAGING_PATH',  'ERROR_PATH'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

rename_map = {
    "cms_certification_number_ccn": "facility_number",
    "provider_name": "facility_name",
    "provider_address": "facility_address",
    "provider_type": "facility_type",
    "deficiency_tag_number": "deficiency_tag",
    "scope_severity_code": "severity_level"
}

# -----------------------------
# Utilities
# -----------------------------
def clean_column_names(df):
    for col_name in df.columns:
        new_col = col_name.strip().lower().replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").replace("-", "_").replace(".", "_")
        df = df.withColumnRenamed(col_name, new_col)
    return df

def clean_data(df):
    for col_name in df.columns:
        df = df.withColumn(col_name, trim(col(col_name)))
    return df

def rename_columns(df, rename_map):
    col_lookup = {col.lower().strip(): col for col in df.columns}
    for old_name, new_name in rename_map.items():
        cleaned_old = old_name.lower().strip()
        if cleaned_old in col_lookup:
            df = df.withColumnRenamed(col_lookup[cleaned_old], new_name)
    return df


# -----------------------------
# Process each domain folder
# -----------------------------
s3 = boto3.client('s3')
bucket = args['SOURCE_PATH'].split('/')[2]
prefix = '/'.join(args['SOURCE_PATH'].split('/')[3:])

response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
folders = [cp['Prefix'].split('/')[-2] for cp in response.get('CommonPrefixes', [])]

if not folders:
    print("No domain folders found.")
    job.commit()
    sys.exit(0)

for folder in folders:
    print(f"Processing folder: {folder}")
    path = f"{args['SOURCE_PATH']}{folder}/"

    try:
        df = spark.read.option("header", True).csv(path)

        if df.rdd.isEmpty():
            print(f"Skipping empty folder: {folder}")
            continue

        df = clean_column_names(df)
        df = rename_columns(df, rename_map)
        df = clean_data(df)
        df = df.withColumn("row_id", monotonically_increasing_id().cast(LongType()))
        df = df.withColumn("etl_date", current_date().cast(DateType()))
        

        staging_path = f"{args['STAGING_PATH']}{folder}/"
        df.write.mode("overwrite").option("header", True).parquet(staging_path)
        print(f"Written cleaned data to staging: {staging_path}")


       

    except Exception as e:
        print(f"Error processing folder {folder}: {e}")
        try:
            error_path = f"{args['ERROR_PATH']}{folder}/"
            df.write.mode("overwrite").option("header", True).parquet(error_path)
            print(f"Wrote failed data to: {error_path}")
        except Exception as nested:
            print(f"Failed to write to error path: {nested}")

job.commit()
