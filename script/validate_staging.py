import boto3
import os
import logging
from botocore.exceptions import BotoCoreError, ClientError

# Set up logging for CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
s3 = boto3.client("s3")
STAGING_BUCKET = os.environ.get("STAGING_BUCKET")
REQUIRED_PREFIXES = os.environ.get("REQUIRED_PREFIXES", "").split(",")

def lambda_handler(event, context):
    logger.info(f"Checking required prefixes in staging bucket: {STAGING_BUCKET}")
    try:
        for prefix in REQUIRED_PREFIXES:
            folder_prefix = f"staging/{prefix.strip()}/"  
            logger.info(f"Checking folder: {folder_prefix}")
            result = s3.list_objects_v2(Bucket=STAGING_BUCKET, Prefix=folder_prefix)

            if result.get("KeyCount", 0) == 0:
                logger.error(f"No files found in: {folder_prefix}")
                return {
                    "status": "error",
                    "message": f"No files found in {folder_prefix}"
                }

        logger.info("All required folders are populated.")
        return {
            "status": "success",
            "message": "All staging folders populated"
        }

    except (BotoCoreError, ClientError) as e:
        logger.exception("S3 error occurred during validation")
        return {
            "status": "error",
            "message": f"S3 error occurred: {str(e)}"
        }
    except Exception as e:
        logger.exception("Unexpected error occurred during validation")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }
