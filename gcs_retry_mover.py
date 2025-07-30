import json
import sys
from google.cloud import storage

def file_matches(filename, match_conditions):
    for condition_set in match_conditions:
        if all(token in filename for token in condition_set):
            return True
    return False

def move_matching_files(match_conditions):
    
    BUCKET_NAME = "msp_data_live"
    BASE_PREFIX = "home/server/scrappeddata/msp_data/"

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = client.list_blobs(BUCKET_NAME, prefix=BASE_PREFIX)

    for blob in blobs:
        blob_name = blob.name
        if "/failed_source_files/" not in blob_name:
            continue

        parts = blob_name.split("/")
        if len(parts) < 7:
            continue

        filename = parts[-1]
        if not file_matches(filename, match_conditions):
            continue

        dest_blob_name = blob_name.replace("/failed_source_files/", "/source_files/")
        try:
            bucket.copy_blob(blob, bucket, new_name=dest_blob_name)
            blob.delete()
            print(f"Moved: {blob_name} → {dest_blob_name}")
        except Exception as e:
            print(f"Failed to move {blob_name}: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 gcs_retry_mover.py '[ [\"shopee\", \"ID\", \"onsiteCampaign\"], [\"lazada\", \"offsite\"] ]'")
        sys.exit(1)

    try:
        match_conditions = json.loads(sys.argv[1])
        if not isinstance(match_conditions, list) or not all(isinstance(c, list) for c in match_conditions):
            raise ValueError("conditions must be a list of lists")
    except Exception as e:
        print(f"Invalid conditions format: {e}")
        raise SystemExit(f"Error: Invalid input format for match conditions: {e}")

    move_matching_files(match_conditions)
    
# python3 gcs_retry_mover.py '[["lazada","ID","onsiteKeyword"],["lazada","offsite"]]'