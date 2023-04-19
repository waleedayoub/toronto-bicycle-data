import os
import json
import requests
import hashlib

from pathlib import Path

from datetime import timedelta

import pandas as pd

from prefect import task, flow

# from prefect_gcp import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_cloud_storage
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash

from utils import download_file, excel_to_csv, extract_zip


# get a json object from the api url
@task(name="fetch_api", log_prints=True, retries=3, retry_delay_seconds=30)
def fetch_api(url: str, params: dict) -> json:
    """Fetch data from an API and return a JSON object"""
    return requests.get(url, params=params).json()


# Define a function to get the data from the url
@task(
    name="download_toronto_ridership_data",
    log_prints=True,
    retries=3,
    retry_delay_seconds=30,
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1),
)
def download_toronto_ridership_data(package: json, file_location: str):
    """Extract the data from the Toronto ridership data package"""
    data_dict = {}
    for _, resource in enumerate(package["result"]["resources"]):
        # To get metadata for non datastore_active resources:
        if not resource["datastore_active"]:
            # Get the URL and file format from the JSON object
            # Note that we could also use the resource_show API (as opposed to the package_show API, but they seem to return the same thing)
            # Earlier data is in XLSX format and later data is a zip of CSVs separated by month
            url = resource["url"]
            file_format = resource["format"]

            # Get the year and filename
            year = url.split("/")[-1].split("-")[-1].split(".")[0]
            filename = url.split("/")[-1]
            print(
                f"we are processing data for url: {url}, year: {year}, format: {file_format} and filename: {filename}"
            )

            # add information to dictionary for later reference
            data_dict[year] = (file_location, filename)

            # Create a directory for the year if it doesn't exist
            if not os.path.exists(f"{file_location}/{year}"):
                os.makedirs(f"{file_location}/{year}")

            # Check if file already exists and hasn't changed since last download
            file_path = f"{file_location}/{year}/{filename}"
            # add information to dictionary
            if os.path.exists(file_path):
                # check hash of existing filename
                with open(file_path, "rb") as f:
                    old_hash = hashlib.sha256(f.read()).hexdigest()
                # check hash of incoming file
                with requests.get(url, stream=True) as r:
                    new_hash = hashlib.sha256(r.content).hexdigest()
                # compare hashes to see if file has changed
                if old_hash == new_hash:
                    print(
                        f"File {filename} already exists and hasn't changed since last download."
                    )
                    continue  # break out of loop and move to next resource

            # Download the file
            download_file(url, f"{file_location}/{year}/{filename}")

            # file handling / extraction:
            # Convert the file to CSV if it's an Excel file
            if file_format == "XLSX":
                excel_to_csv(filename, f"{file_location}/{year}")
            elif file_format == "ZIP":
                extract_zip(filename, f"{file_location}/{year}")

    return data_dict


# create a function to load data to gcs from local
@task(name="load_to_gcs", log_prints=True, retries=3, retry_delay_seconds=30)
def load_to_gcs(file_location: str, year: str, file: str) -> Path:
    """Convert local data to parquet and upload to a GCS bucket, return path to the bucket"""
    print(f"uploading data to gcs for {file_location}/{year}/{file}")

    local_file_path = Path(f"{file_location}/{year}/{file}")

    try:
        df = pd.read_csv(local_file_path)
    except Exception as e:
        print(f"Error while doing read_csv() on file {local_file_path}: {e}")
        return None
    # df.to_parquet(local_file_path)
    gcs_path = f"{file_location}/{year}/{file}.parquet"

    # upload directly from dataframe and skip the write local step:
    gcs_connection_block = GcsBucket.load("toronto-bikeshare-gcs")
    try:
        gcs_connection_block.upload_from_dataframe(
            df=df, to_path=gcs_path, serialization_format="parquet"
        )
    except Exception as e:
        print(
            f"Error while doing upload_from_dataframe on {local_file_path} dataframe to gcs: {e}"
        )
        return None
    # if the above doesn't work, uncomment the step where you write the file to parquet locally
    # gcs_connection_block.upload_from_path(from_path=local_file_path, to_path=gcs_path)

    return Path(f"{gcs_path}")


# create a function to load data to bq from gcs
@task(name="load_to_bq", log_prints=True, retries=3, retry_delay_seconds=30)
def load_to_bq(file_path: Path):
    """Load data from GCS to BigQuery"""
    # load gcp credentials
    gcp_credentials_block = GcpCredentials.load("de-gcp-creds")
    result = bigquery_load_cloud_storage(
        dataset="toronto_bikeshare",
        table="stg_toronto_ridership",
        uri=file_path,
        gcp_credentials=gcp_credentials_block.get_credentials_from_service_account(),
    )
    return result


@flow(name="toronto_ridership_main_flow", log_prints=True)
def toronto_ridership_main_flow(
    package_url: str, params: dict, file_location: str
) -> None:
    # call fetch_api to get the json object with the correct parameters
    # this function should be usable for all ckan hosted packages
    package = fetch_api(package_url, params)

    # download and do some file cleanup for all ridership data, return a dict
    data_dict = download_toronto_ridership_data(package, file_location)
    # load data from local to GCS bucket
    for year, (file_location, filename) in data_dict.items():
        # only load data for years 2016 or greater
        if year.isdigit() and int(year) >= 2016:
            for file in os.listdir(f"{file_location}/{year}"):
                if file.endswith(".csv") or file.endswith(".zip") and file != filename:
                    # gcs_path = load_to_gcs(file_location, year, file)
                    # load_to_bq(gcs_path)
                    load_to_gcs(file_location, year, file)


if __name__ == "__main__":
    # Toronto Open Data is stored in a CKAN instance. It's APIs are documented here:
    # https://docs.ckan.org/en/latest/api/

    # The base url for the toronto bike share data is hosted by ckan
    # Datasets are called "packages". Each package can contain many "resources"
    # To retrieve the metadata for this package and its resources, use the package name in this page's URL:
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    package_url = base_url + "/api/3/action/package_show"
    params = {"id": "bike-share-toronto-ridership-data"}
    file_location = f"data/toronto/ridership"

    toronto_ridership_main_flow(package_url, params, file_location)
