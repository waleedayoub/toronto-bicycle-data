import os
import json
import requests
import hashlib
import zipfile
import shutil

from pathlib import Path

from datetime import timedelta

import pandas as pd

from prefect import task, flow
from prefect_gcp import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_cloud_storage
from prefect.tasks import task_input_hash


# get a json object from the api url
@task(name="fetch_api", log_prints=True, retries=3, retry_delay_seconds=30)
def fetch_api(url: str, params: dict) -> json:
    """Fetch data from an API and return a JSON object"""
    return requests.get(url, params=params).json()


# download data to a file from a url
@task(name="download_file", log_prints=True, retries=3, retry_delay_seconds=30)
def download_file(url: str, filename: str) -> None:
    """Download a file from a URL and save it to disk"""
    response = requests.get(url, stream=True)
    try:
        with response as r:
            r.raise_for_status()
            with open(filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except requests.exceptions.RequestException as err:
        print(f"Error downloading file {url}: {err}")
        raise


# Define a function to save each tab in an Excel file as a CSV file
@task(name="excel_to_csv", log_prints=True, retries=3, retry_delay_seconds=30)
def excel_to_csv(filename: str, file_location: str) -> None:
    """Go through each worksheet in an Excel workbook and convert each sheet to a CSV file"""
    # Load the Excel file into a Pandas dataframe
    xl = pd.ExcelFile(f"{file_location}/{filename}")
    # Loop through each sheet in the Excel file
    for sheet_name in xl.sheet_names:
        # Load the sheet into a Pandas dataframe
        df = xl.parse(sheet_name)
        # Save the sheet as a CSV file
        csv_filename = f"{sheet_name}.csv"
        df.to_csv(f"{file_location}/{csv_filename}", index=False)


# task that extracts zip files (should move this to utils.py)
@task(name="extract_zip", log_prints=True, retries=3, retry_delay_seconds=30)
def extract_zip(filename: str, file_location: str) -> None:
    """Extract a zip file to a temporary directory and then move the files to file_location"""
    with zipfile.ZipFile(f"{file_location}/{filename}", "r") as zip_ref:
        temp_dir = os.path.join(file_location, "temp")
        zip_ref.extractall(temp_dir)
        # Move the extracted files to file_location
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                shutil.move(os.path.join(root, file), os.path.join(file_location, file))

        # Remove the temporary directory
        # os.rmdir(temp_dir)


# Define a function to get the data from the url
@task(
    name="download_toronto_ridership_data",
    log_prints=True,
    retries=3,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
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

            # Create a directory for the year if it doesn't exist
            if not os.path.exists(f"{file_location}/{year}"):
                os.makedirs(f"{file_location}/{year}")

            # Check if file already exists and hasn't changed since last download
            file_path = f"{file_location}/{year}/{filename}"
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

            # add information to dictionary
            data_dict[year] = (file_location, filename)
    return data_dict


# create a function to load data to gcs from local
@task(name="load_to_gcs", log_prints=True, retries=3, retry_delay_seconds=30)
def load_to_gcs(file_location: str, year: str, file: str) -> Path:
    """Convert local data to parquet and upload to a GCS bucket, return path to the bucket"""
    local_file_path = Path("data/{file_location}/{year}/{file}")
    df = pd.read_csv(local_file_path)
    # df.to_parquet(local_file_path)
    gcs_path = f"data/{file_location}/{year}/{file}.parquet"

    print(f"uploading data to gcs for {file_location}/{year}/{file}")
    # upload directly from dataframe and skip the write local step:
    gcs_connection_block = GcsBucket.load("toronto-bikeshare-gcs")
    gcs_connection_block.upload_from_dataframe(
        df=df, to_path=gcs_path, serialization_format="parquet"
    )
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
        dataset="",
        table="",
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
                    gcs_path = load_to_gcs(file_location, year, file)
                    load_to_bq(gcs_path)


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
