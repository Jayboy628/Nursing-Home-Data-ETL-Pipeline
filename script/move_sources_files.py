import boto3
import os

s3 = boto3.client("s3")

def lambda_handler(event, context):
    SOURCE_BUCKET = os.environ["SOURCE_BUCKET"]
    PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
    ERROR_BUCKET = os.environ["ERROR_BUCKET"]
    PREFIX = os.environ.get("PREFIX", "")
    S3_FOLDERS = os.environ.get("S3_FOLDERS", "").split(",")

    print(f"Starting move from: {SOURCE_BUCKET}/{PREFIX}")
    
    # List folders under raw/
    all_folders_response = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=f"{PREFIX}raw/", Delimiter="/")
    all_folders = [prefix["Prefix"].split("/")[1] for prefix in all_folders_response.get("CommonPrefixes", [])]

    known_folders = set(S3_FOLDERS)
    all_folders_set = set(all_folders)

    for folder in all_folders_set:
        folder_prefix = f"{PREFIX}raw/{folder}/" if PREFIX else f"raw/{folder}/"
        target_bucket = PROCESSED_BUCKET if folder in known_folders else ERROR_BUCKET

        print(f"Processing folder: {folder} -> {target_bucket}")
        response = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=folder_prefix)
        if "Contents" not in response:
            print(f"No files found in {folder_prefix}")
            continue

        for obj in response["Contents"]:
            key = obj["Key"]
            s3.copy_object(
                Bucket=target_bucket,
                CopySource={"Bucket": SOURCE_BUCKET, "Key": key},
                Key=key
            )
            s3.delete_object(Bucket=SOURCE_BUCKET, Key=key)
            print(f"Moved {key} to {target_bucket}")

    return {"status": "Completed folder move including unknown folders to error bucket"}
