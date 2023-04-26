# Toronto Bicycle Data Engineering

- This is my submission for the [final project](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_7_project/README.md) of the datatalks club data engineering zoomcamp

## Project Description
- The goal of this project is to create a comparative analytical analysis of a few bike share programs in Canada.
- Given data availability, these programs will be those located in 3 cities: Toronto, Montreal and Vancouver. The initial focus of this analysis will be the city of Toronto, with successive implementations to include the other two cities as well.
- For the city of Toronto, the project will consist of 2 main pipelines:
    - a batch pipeline that runs monthly and sources data from ridership data
    - a near real time pipeline that runs every 5 minutes and sources data from the gbfs API
    - See data sources below for more information

## Architecture
- Initially, this project will deal with data in batch as the primary data sources we are dealing with get updated monthly
- The bike share programs do seem to have real-time data feeds for the bike locations, but we will revisit that at a later time
- We are going to create 2 pipeline flows: dev and prod
    - dev:
        - the dev pipeline will deploy using locally hosted components
        - no terraform, postgres, prefect orion, dbt core, metabase
    - prod:
        - the prod pipeline will deploy using mostly hosted components in gcp
        - terraform, prefect cloud, dbt cloud, gcs, bq, looker

## Data sources
### Toronto
- Ridership data: https://open.toronto.ca/dataset/bike-share-toronto-ridership-data/

## Deployment instructions

### Technologies used
- GCP / Cloud Storage / BigQuery / Looker
- Terraform
- Prefect / DBT
- Python 3.9.16 / virtualenv

### Things you need to install + versions
- Google cloud SDK: https://cloud.google.com/sdk/docs/install
- Terraform 1.4.5: https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli
- Python: make sure you're running 3.9.16
- Prefect 2.10.4: https://docs.prefect.io/latest/getting-started/installation/
    - It is *very* important to get the prefect version right as GCS block's ```upload_from_dataframe()``` [method](https://prefecthq.github.io/prefect-gcp/cloud_storage/#prefect_gcp.cloud_storage.GcsBucket.upload_from_dataframe) doesn't work in older versions

### Step 0
- Clone or copy this repo

### Step 1 - Initial Setup + GCP
1. Create a service account in GCP and download the service account json (In the IAM & Admin section of the GCP console)
    - Make sure the service account has the following roles assigned:
    - [IAM Roles](https://cloud.google.com/storage/docs/access-control/iam-roles) for Service account:
    - Go to the *IAM* section of *IAM & Admin* https://console.cloud.google.com/iam-admin/iam
    - Click the *Edit principal* icon for your service account.
    - Add these roles in addition to *Viewer* : **Storage Admin** + **Storage Object Admin** + **BigQuery Admin**
   
2. Enable these APIs for your project:
   - https://console.cloud.google.com/apis/library/iam.googleapis.com
   - https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
3. Ensure your environment variable is pointing to the .json file you downloaded from the GCP console, refresh your token session and verify the authentication. Here are the steps:
```shell
# Set your environment variable to where your .json file is located
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"

# Refresh token/session, and verify authentication
gcloud auth application-default login
```
- Now you're ready to provision the services we'll need, using Terraform.

### Step 2 - Terraform setup
1. In the ```variables.tf``` file, modify the "project" variable description with the name of your GCP project:
```shell
variable "project" {
  description = "possible-lotus-375803"
}
```
2. Run the following:
```shell
cd terraform
terraform init
terraform apply
```
- You'll prompted to select your GCP project to proceed and provision the resources

### Step 3 - Install python requirements
- Run ```pip install -r requirements.txt```

### Step 4 - Run end to end pipeline for all ridership data using Prefect
1. Ensure you have an account on [app.prefect.cloud](app.prefect.cloud)
2. Create 2 blocks in prefect:
    - GCP credentials block with your GCP project ID and key from your service account .json file
    - GCS bucket block using the name of the bucket in the terraform ```dtc-toronto-bikeshare``` and the name of your GCP credentials block above
3. In prefect cloud, grab an API key
4. Run ```prefect cloud login```
    - You can follow instructions or just copy in the API key from step 3
5. Run ```python toronto_ridership.py```
6. Wait for all steps in the DAG to complete

### Step 5 - Load data to BigQuery
1. Once data is ready in your data lake, you can load data to bigquery tables
2. Run the following code:
```shell
bq query --use_legacy_sql=false --project_id=possible-lotus-375803 --location=northamerica-northeast1 --format=prettyjson < bq_reporting.sql
```

- And there you have it, all ridership data is available in BQ external tables ready for querying