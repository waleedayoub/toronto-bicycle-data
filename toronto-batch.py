import requests
import os
import pandas as pd

# Toronto Open Data is stored in a CKAN instance. It's APIs are documented here:
# https://docs.ckan.org/en/latest/api/


# Define a function to download the data from the URL and save it to a file
def download_file(url, filename):
    response = requests.get(url)
    with open(filename, "wb") as f:
        f.write(response.content)


# Function to download data and save to folder structure
# city/year/{month}.csv
def get_toronto_bikeshare_ridership(base_url, package):
    for _, resource in enumerate(package["result"]["resources"]):
        # To get metadata for non datastore_active resources:
        if not resource["datastore_active"]:
            url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
            item = requests.get(url).json()

            # Get the URL and file format from the JSON object
            # Earlier data is in XLSX format and later data is a zip of CSVs separated by month
            url = item["result"]["url"]
            file_format = item["result"]["format"]
            year = url.split("/")[-1].split("-")[-1].split(".")[0]
            print(f"procesing data for: {url}")
            print(f"file format: {file_format} and year: {year}")

            # create a directory for the year if it doesn't exist
            if not os.path.exists(f"data/toronto/{year}"):
                os.makedirs(f"data/toronto/{year}")
            # Download the file for each year
            filename = f'data/toronto/{year}/{item["result"]["name"]}.{file_format}'
            download_file(url, filename)

            # If the file_format is XLSX then open XLSX and save each tab as a CSV in the directory
            # If the file_format is ZIP then extract contents and save each CSV in the directory
            if file_format == "XLSX":
                # Read in the xlsx file and save each tab as a CSV file
                xl = pd.read_excel(file_path, sheet_name=None)
                for sheet_name, df in xl.items():
                    csv_path = os.path.join(f"data/toronto/{year}", f"{sheet_name}.csv")
                    df.to_csv(csv_path, index=False)
            elif file_extension == "ZIP":
                # Extract zip contents into the same directory
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(f"data/toronto/{year}")


if __name__ == "__main__":
    # To hit our API, you'll be making requests to:
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

    # Datasets are called "packages". Each package can contain many "resources"
    # To retrieve the metadata for this package and its resources, use the package name in this page's URL:
    url = base_url + "/api/3/action/package_show"
    params = {"id": "bike-share-toronto-ridership-data"}
    package = requests.get(url, params=params).json()
    get_toronto_bikeshare(base_url, package)
