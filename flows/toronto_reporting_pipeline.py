from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_cloud_storage

# create a function to load data by year from GCS to bigquery
@task(name="load_to_bq", log_prints=True, retries=3, retry_delay_seconds=30)
def load_to_bq(file_path: Path):
    """Load data by year from GCS to BigQuery"""
    # load gcp credentials
    gcp_credentials_block = GcpCredentials.load("de-gcp-creds")
    result = bigquery_load_cloud_storage(
        dataset="toronto_bikeshare",
        table="stg_toronto_ridership",
        uri=file_path,
        gcp_credentials=gcp_credentials_block.get_credentials_from_service_account(),
    )
    return result

@flow(name="toronto_reporting_flow", log_prints=True)
def toronto_reporting_flow(years: list) -> None:
    for year in years:
        load_to_bq(year, dataset_name)
    return None

if __name__ == "__main__":
    years = [2016:2022]
    dataset_name = "toronto_bikeshare"
    toronto_reporting_flow(years, dataset_name)