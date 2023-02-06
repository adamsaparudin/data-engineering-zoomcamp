from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("nyc-taxi")
    gcs_block.get_directory(from_path=gcs_path, local_path="./")
    return Path(f"./{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("devil-course-nyc-taxi")

    df.to_gbq(
        destination_table="de.trips",
        project_id="nyc-taxi-course",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(color: str, month: int, year: int) -> int:
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    return len(df)


@flow(log_prints=True)
def etl_parent_gcs_bq(
    color: str = "yellow", months: list[int] = [1, 2], year: int = 2021
):
    total = 0
    for month in months:
        row_count = etl_gcs_to_bq(color, month, year)
        total += row_count
    print(f"Total processed data {total}")


if __name__ == "__main__":
    months = [2, 3]
    color = "yellow"
    year = 2019
    etl_parent_gcs_bq(color, months, year)
