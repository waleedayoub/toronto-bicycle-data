import requests

# This API contains station level information for the Toronto Bike Share program.
# The data is updated frequently and can be used for real time analysis of the program.

# Toronto Open Data is stored in a CKAN instance. It's APIs are documented here:
# https://docs.ckan.org/en/latest/api/

# To hit our API, you'll be making requests to:
base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

# Datasets are called "packages". Each package can contain many "resources"
# To retrieve the metadata for this package and its resources, use the package name in this page's URL:
url = base_url + "/api/3/action/package_show"
params = {"id": "bike-share-toronto"}
package = requests.get(url, params=params).json()
resource_metadata = []

# To get resource data:
for idx, resource in enumerate(package["result"]["resources"]):
    # To get metadata for non datastore_active resources:
    if not resource["datastore_active"]:
        url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
        resource_metadata.append(requests.get(url).json())
        # print(resource_metadata)
        # From here, you can use the "url" attribute to download this file

for _, item in enumerate(resource_metadata):
    if item["result"]["name"] == "bike-share-json":
        print(item["result"]["url"])
        bike_share_json = requests.get(item["result"]["url"]).json()


# bike_share_json = requests.get(resource_metadata[0]["result"]["url"]).json()
print(bike_share_json)
