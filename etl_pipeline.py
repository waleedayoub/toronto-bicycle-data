from pathlib import Path
from datetime import timedelta

import pandas as pd

from prefect import task, flow
from prefect_gcp import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_cloud_storage
from prefect.tasks import task_input_hash


# extract data from url and return a dataframe
@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_to_gcs(url: str, colour: str, file_name: str) -> Path:
    """Extract data from website, convert to pandas dataframe -> parquet -> load to GCS"""

    # read the data into a dataframe
    df = pd.read_csv(url)

    local_file_path = Path(
        f"/Users/waleed/Documents/school/datatalksclub/data-engineering-zoomcamp/week_2_workflow_orchestration/data/{colour}taxidata/{file_name}.parquet"
    )
    # convert dataframe to parquet
    df.to_parquet(local_file_path, compression="gzip")

    gcs_path = f"data/{colour}/{file_name}.parquet"
    gcs_connection_block = GcsBucket.load("taxidata-gcs")
    gcs_connection_block.upload_from_path(from_path=local_file_path, to_path=gcs_path)

    return Path(f"{gcs_path}")


# load to bigquery task
@task(log_prints=True)
def load_to_bq(file_path: Path):
    """Load task to write data to bigquery table"""

    # load gcp credentials
    gcp_credentials_block = GcpCredentials.load("de-gcp-creds")
    result = bigquery_load_cloud_storage(
        dataset="possible-lotus-375803.trips_data_all",
        table="possible-lotus-375803.trips_data_all.yellowtaxidata",
        uri=file_path,
        gcp_credentials=gcp_credentials_block.get_credentials_from_service_account(),
    )
    return result


# main flow
@flow(name="GCS to BQ subflow", log_prints=True)
def etl_gcs_to_bq(taxi_colour, year, month):
    """ETL subflow to load data from gcs to bigquery"""

    file_name = f"{taxi_colour}_tripdata_{year}-{month:02}"
    file_location = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_colour}/{file_name}.csv.gz"

    # get data from the web, save locally and upload to GCS, return path to GCS
    gcs_path = extract_to_gcs(file_location, taxi_colour, file_name)

    # load the data to BQ using load_cloud_storage
    load_to_bq(gcs_path)


@flow(name="Load to BQ main flow", log_prints=True)
def bq_main_flow(taxi_colour: str, year: int, months: list[int]) -> None:
    """Main flow that calls the subflow"""
    for month in months:
        etl_gcs_to_bq(taxi_colour, year, month)


if __name__ == "__main__":
    taxi_colour = "yellow"
    year = 2019
    months = [2, 3]
    bq_main_flow(taxi_colour, year, months)
