import sys
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, monotonically_increasing_id, current_date
from pyspark.sql.types import LongType, DateType

# Glue args
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'STAGING_PATH',
    'TRANSFORM_PATH'
])

# Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


try:
    # Read cleaned data from staging
    staging_domain = "qualitymsr_mds"
    df = spark.read.parquet(f"{args['STAGING_PATH']}{staging_domain}/")

    # Base ID and drop columns
    pk = "facility_number"
    drop_cols = ['facility_name', 'provider_address', 'city_town', 'zip_code']
    df = df.drop(*[c for c in drop_cols if c in df.columns])

    # Select relevant columns
    df_qualitymsr = df.select(
        pk, "measure_code", "measure_description", "resident_type",
        "q1_measure_score", "footnote_for_q1_measure_score",
        "q2_measure_score", "footnote_for_q2_measure_score",
        "q3_measure_score", "footnote_for_q3_measure_score",
        "q4_measure_score", "footnote_for_q4_measure_score",
        "four_quarter_average_score", "footnote_for_four_quarter_average_score",
        "used_in_quality_measure_five_star_rating",
        "measure_period", "location", "processing_date"
    )

    # Add row_id + etl_date
    df_qualitymsr = df_qualitymsr.withColumn("row_id", monotonically_increasing_id().cast(LongType()))
    df_qualitymsr = df_qualitymsr.withColumn("etl_date", current_date().cast(DateType()))

    # Write to transform
    df_qualitymsr.write.mode("overwrite").parquet(f"{args['TRANSFORM_PATH']}{staging_domain}/")
    print(f"Wrote df_qualitymsr to transform: {args['TRANSFORM_PATH']}{staging_domain}/")

except Exception as e:
    print(f"Error during transform: {e}")
    try:
        if 'df' in locals():
            error_path = f"{args['ERROR_PATH']}{staging_domain}/"
            df.write.mode("overwrite").parquet(error_path)
            print(f"Wrote raw df to error path: {error_path}")
    except Exception as nested:
        print(f"Failed to write to error path: {nested}")

job.commit()
