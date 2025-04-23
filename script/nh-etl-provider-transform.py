import sys
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, monotonically_increasing_id, current_date

# Glue args
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'STAGING_PATH',
    'TRANSFORM_PATH'
])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Read cleaned provider data from staging
df = spark.read.parquet(f"{args['STAGING_PATH']}provider_info/")

# Base ID and common drop columns
pk = "facility_number"
drop_cols = ['facility_name', 'facility_address', 'city_town', 'zip_code']

# -------------------
# df_facility
# -------------------
df_facility = df.select(
    pk,
    "facility_name",
    "facility_address",
    "city_town",
    "state",
    "zip_code",
    "telephone_number",
    "provider_ssa_county_code",
    "county_parish",
    "ownership_type",
    "number_of_certified_beds",
    "average_number_of_residents_per_day",
    "average_number_of_residents_per_day_footnote",
    "facility_type",
    "provider_resides_in_hospital",
    "legal_business_name",
    "date_first_approved_to_provide_medicare_and_medicaid_services",
    "affiliated_entity_name",
    "affiliated_entity_id",
    "continuing_care_retirement_community",
    "special_focus_status",
    "abuse_icon",
    "row_id",
    "etl_date"
)
df_facility.write.mode("overwrite").parquet(f"{args['TRANSFORM_PATH']}facility/")
print("Wrote df_facility to transform")

# -------------------
# df_staffing
# -------------------
df_staffing = df.select(*[col_name for col_name in df.columns if any(k in col_name for k in [
    pk, "staffing", "hours_per", "turnover", "case_mix", "adjusted"
])]).drop(*[c for c in drop_cols if c in df.columns])

# -------------------
# df_rating
# -------------------
df_rating = df.select(*[c for c in df.columns if "rating" in c or "footnote" in c or pk in c]).drop(*[c for c in drop_cols if c in df.columns])

# -------------------
# df_surveys
# -------------------
df_surveys = df.select(*[c for c in df.columns if any(k in c for k in [
    "rating_cycle", "health_deficiency", "revisit_score", "total_weighted_health_survey_score", pk
])]).drop(*[c for c in drop_cols if c in df.columns])

df_survey_summary = spark.read.option("header", True).parquet(f"{args['STAGING_PATH']}survey_summary/")
df_survey_summary = df_survey_summary.drop(*[c for c in drop_cols if c in df_survey_summary.columns])
df_survey_summary = df_survey_summary.withColumnRenamed("facility_number", pk)

df_surveys_joined = df_surveys.join(df_survey_summary, on=pk, how="left")

# -------------------
# df_penalties
# -------------------
df_penalties = df.select(
    pk,
    "number_of_facility_reported_incidents",
    "number_of_substantiated_complaints",
    "number_of_citations_from_infection_control_inspections",
    "number_of_fines",
    "total_amount_of_fines_in_dollars",
    "number_of_payment_denials",
    "total_number_of_penalties"
)

df_penalties_ext = spark.read.option("header", True).parquet(f"{args['STAGING_PATH']}penalties/")
df_penalties_ext = df_penalties_ext.drop(*[c for c in drop_cols if c in df_penalties_ext.columns])
df_penalties_ext = df_penalties_ext.withColumnRenamed("facility_number", pk)

df_penalties_joined = df_penalties.join(df_penalties_ext, on=pk, how="left")

# -------------------
# Write other outputs
# -------------------
output_frames = {
    "staffing": df_staffing,
    "rating": df_rating,
    "surveys": df_surveys_joined,
    "penalties": df_penalties_joined
}

for name, frame in output_frames.items():
    frame = frame.withColumn("row_id", monotonically_increasing_id())
    frame = frame.withColumn("etl_date", current_date())
    frame.write.mode("overwrite").parquet(f"{args['TRANSFORM_PATH']}{name}/")
    print(f"Wrote df_{name} to transform")


job.commit()
