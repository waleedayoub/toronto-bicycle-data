import requests
import csv

# The URL of the 'station_information' feeds
url = "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_information"

# The name of the CSV file to save the data to
csv_filename = "station_information.csv"

# Download the contents of the URL
response = requests.get(url)

# Parse the JSON response
data = response.json()

# Extract the station information from the JSON data
station_info = data["data"]["en"]["stations"]

# Open the CSV file for writing
with open(csv_filename, "w", newline="") as csvfile:
    # Create a CSV writer object
    writer = csv.writer(csvfile)

    # Write the header row
    writer.writerow(["station_id", "name", "lat", "lon", "capacity"])

    # Write each row of station information to the CSV file
    for station in station_info:
        writer.writerow(
            [
                station["station_id"],
                station["name"],
                station["lat"],
                station["lon"],
                station["capacity"],
            ]
        )
