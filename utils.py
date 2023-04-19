import requests
import pandas as pd
import zipfile
import os
import shutil


# download data to a file from a url
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
def extract_zip(filename: str, file_location: str) -> None:
    """Extract a zip file to a temporary directory and then move the files to file_location"""
    with zipfile.ZipFile(f"{file_location}/{filename}", "r") as zip_ref:
        temp_dir = os.path.join(file_location, "temp")
        zip_ref.extractall(temp_dir)
        # Move the extracted files to file_location
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                shutil.move(os.path.join(root, file), os.path.join(file_location, file))
