import io
import os
import requests
from google.cloud import storage

# services = ['fhv','green','yellow']
base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_possible-lotus-375803")

print(f"the fhv base url is {base_url} and i upload to {BUCKET}")


# create connection to GCS and upload the file
def upload_to_gcs(bucket, object_name, file_location):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_string(file_location)


# parse the file location and call upload to gcs
def web_to_gcs(year: int, service: str) -> None:
    for i in range(12):
        # sets the month part of the file_name string
        month = "0" + str(i + 1)
        month = month[-2:]

        # csv file_name
        file_name = service + "_tripdata_" + str(year) + "-" + month + ".csv.gz"

        # download it using requests
        file_location = base_url + service + "/" + file_name
        response = requests.get(file_location)
        print(f"Web: {file_location}")

        # upload it to gcs
        upload_to_gcs(BUCKET, f"data/{service}/{file_name}", response.content)
        print(f"GCS: data/{service}/{file_name}")


if __name__ == "__main__":
    web_to_gcs(2019, "fhv")
