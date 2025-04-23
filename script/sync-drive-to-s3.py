import json
import os
import io
import boto3
import time
import logging
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

s3 = boto3.client("s3")
MANIFEST_KEY = "metadata/processed_files.json"

def get_env_var(name):
    value = os.environ.get(name)
    if not value:
        raise KeyError(f"Missing required environment variable: {name}")
    return value

def load_manifest(s3_bucket):
    try:
        response = s3.get_object(Bucket=s3_bucket, Key=MANIFEST_KEY)
        return json.load(response["Body"])
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return []
        raise

def update_manifest(s3_bucket, manifest):
    s3.put_object(
        Bucket=s3_bucket,
        Key=MANIFEST_KEY,
        Body=json.dumps(manifest, indent=2).encode("utf-8")
    )

def determine_s3_path(file_name, config):
    normalized = file_name.replace("NH_", "").replace("_", "").lower()
    for pattern, path in config["FILE_TYPE_MAPPING"].items():
        if pattern.lower() in normalized:
            return path
    return config["FILE_TYPE_MAPPING"].get("_DEFAULT", "raw/other/")

def file_in_manifest(file_id, manifest):
    return any(item["file_id"] == file_id for item in manifest)

def retry(func, retries=3, delay=5):
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            logging.warning(f"Retry {i+1}/{retries} failed: {e}")
            time.sleep(delay)
    raise RuntimeError("Max retries exceeded")

def download_file_content(file, drive_service):
    if file["mimeType"] == "application/vnd.google-apps.spreadsheet":
        request = drive_service.files().export_media(fileId=file["id"], mimeType="text/csv")
        file_name = f"{file['name']}.csv"
    else:
        request = drive_service.files().get_media(fileId=file["id"])
        file_name = file["name"]
    return file_name, request

def upload_to_s3(buffer, s3_bucket, s3_path, file_name, dry_run):
    if dry_run:
        logging.info(f"[Dry Run] Would upload: {file_name}")
        return
    s3.upload_fileobj(
        Fileobj=buffer,
        Bucket=s3_bucket,
        Key=f"{s3_path}{file_name}",
        ExtraArgs={"ContentType": "text/csv"}
    )

def lambda_handler(event, context):
    # Load env vars inside the function
    S3_BUCKET = get_env_var("SOURCE_BUCKET")
    CONFIG_KEY = get_env_var("CONFIG_KEY")
    SA_KEY = get_env_var("SERVICE_ACCOUNT_KEY")
    DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

    # Load config and credentials
    CONFIG = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=CONFIG_KEY)["Body"].read())
    SA_JSON = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=SA_KEY)["Body"].read())

    creds = service_account.Credentials.from_service_account_info(
        SA_JSON, scopes=["https://www.googleapis.com/auth/drive"]
    )
    drive_service = build("drive", "v3", credentials=creds)

    manifest = load_manifest(S3_BUCKET)

    query = f"'{CONFIG['GOOGLE_DRIVE_FOLDER_ID']}' in parents and (mimeType='text/csv' or mimeType='application/vnd.google-apps.spreadsheet')"
    files = drive_service.files().list(
        q=query,
        fields="files(id, name, mimeType, modifiedTime)",
        pageSize=1000,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute().get("files", [])

    new_manifest = list(manifest)

    for file in files:
        if file_in_manifest(file["id"], manifest):
            logging.info(f"Already synced: {file['name']}")
            continue

        file_name, content = download_file_content(file, drive_service)
        s3_path = determine_s3_path(file_name, CONFIG)

        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, content)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        buffer.seek(0)

        retry(lambda: upload_to_s3(buffer, S3_BUCKET, s3_path, file_name, DRY_RUN))
        logging.info(f"Uploaded: {file_name} → {s3_path}")

        new_manifest.append({
            "file_id": file["id"],
            "file_name": file_name,
            "s3_key": f"{s3_path}{file_name}",
            "synced_at": datetime.utcnow().isoformat() + "Z"
        })

    if not DRY_RUN:
        update_manifest(S3_BUCKET, new_manifest)
        logging.info("Manifest updated.")

    return {"status": "sync complete", "files_synced": len(new_manifest) - len(manifest)}
