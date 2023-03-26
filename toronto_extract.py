import os
import json
import requests
import pandas as pd
import xlsxwriter

# Toronto Open Data is stored in a CKAN instance. It's APIs are documented here:
# https://docs.ckan.org/en/latest/api/

# The base url for the toronto bike share data is hosted by ckan
# Datasets are called "packages". Each package can contain many "resources"
# To retrieve the metadata for this package and its resources, use the package name in this page's URL:
base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
url = base_url + "/api/3/action/package_show"
params = {"id": "bike-share-toronto-ridership-data"}


def get_toronto_data(params: dict) -> json:
    package = requests.get(url, params=params).json()

    for idx, resource in enumerate(package["result"]["resources"]):
        # To get metadata for non datastore_active resources:
        if not resource["datastore_active"]:
            url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
            resource_metadata = requests.get(url).json()
    return resource_metadata


# Define a function to download the data from the URL and save it to a file
def download_file(url, filename):
    response = requests.get(url)
    with open(filename, "wb") as f:
        f.write(response.content)


# Define a function to save each tab in an Excel file as a CSV file
def excel_to_csv(filename):
    # Load the Excel file into a Pandas dataframe
    xl = pd.ExcelFile(filename)
    # Loop through each sheet in the Excel file
    for sheet_name in xl.sheet_names:
        # Load the sheet into a Pandas dataframe
        df = xl.parse(sheet_name)
        # Save the sheet as a CSV file
        csv_filename = f"{sheet_name}.csv"
        df.to_csv(csv_filename, index=False)


# Load the JSON data
with open("data.json", "r") as f:
    data = json.load(f)

# Loop through each JSON object
for item in data:
    # Get the URL and format from the JSON object
    url = item["result"]["url"]
    file_format = item["result"]["format"]
    # Get the year from the filename
    year = url.split("/")[-1].split("-")[-1].split(".")[0]
    # Create a directory for the year if it doesn't exist
    if not os.path.exists(year):
        os.makedirs(year)
    # Download the file
    filename = f"{year}/{item['result']['name']}"
    download_file(url, filename)
    # Convert the file to CSV if it's an Excel file
    if file_format == "XLSX":
        excel_to_csv(filename)