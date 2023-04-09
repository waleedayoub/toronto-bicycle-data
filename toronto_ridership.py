import os
import json
import requests
import pandas as pd
import hashlib
import zipfile
import shutil


# get a json object from the api url
def fetch_api(url: str, params: dict) -> json:
    return requests.get(url, params=params).json()


# download data to a file from a url
def download_file(url: str, filename: str) -> None:
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
def excel_to_csv(filename: str, file_location: str) -> None:
    # Load the Excel file into a Pandas dataframe
    xl = pd.ExcelFile(f"{file_location}/{filename}")
    # Loop through each sheet in the Excel file
    for sheet_name in xl.sheet_names:
        # Load the sheet into a Pandas dataframe
        df = xl.parse(sheet_name)
        # Save the sheet as a CSV file
        csv_filename = f"{sheet_name}.csv"
        df.to_csv(f"{file_location}/{csv_filename}", index=False)


def extract_zip(filename: str, file_location: str) -> None:
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
def extract_toronto_ridership_data(package: json, file_location: str):
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
                with open(file_path, "rb") as f:
                    old_hash = hashlib.sha256(f.read()).hexdigest()
                with requests.get(url, stream=True) as r:
                    new_hash = hashlib.sha256(r.content).hexdigest()
                if old_hash == new_hash:
                    print(
                        f"File {filename} already exists and hasn't changed since last download."
                    )
                    continue

            # Download the file
            download_file(url, f"{file_location}/{year}/{filename}")

            # file handling / extraction:
            # Convert the file to CSV if it's an Excel file
            if file_format == "XLSX":
                excel_to_csv(filename, f"{file_location}/{year}")
            elif file_format == "ZIP":
                extract_zip(filename, f"{file_location}/{year}")

            # Clean the headers of the files and prepare them for upload to gcs


# create a function to load data to gcs from local
def load_to_gcs(file_location: str):
    print(file_location)
    pass


# create a function to load data to bq from gcs
def load_to_bq():
    pass


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

    # call fetch_api to get the json object with the correct parameters
    # this function should be usable for all ckan hosted packages
    package = fetch_api(package_url, params)
    # extract data from the json objects and perform some data cleanup
    extract_toronto_ridership_data(package, file_location)
    # load data from local to GCS bucket
    # load_to_gcs(file_location)
