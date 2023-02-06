from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFram"""

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame):
    """Clean dtype"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(df.dtypes)
    print(f"rows: {len(df)}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file"""
    path = Path(f"./{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(timeout_seconds=600)
def write_gcs(path: Path) -> None:
    """Upload local parquet to GCS"""
    gcs_bucket = GcsBucket.load("nyc-taxi")
    gcs_bucket.upload_from_path(from_path=path, to_path=path, timeout=120)
    return


@flow(timeout_seconds=600)
def etl_web_to_gcs(month: int, year: int, color: str) -> int:
    """Main ETL Function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"  # GITHUB
    # dataset_url = f"~/Downloads/{dataset_file}.csv.gz" # LOCAL

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)
    return len(df_clean)


@flow(log_prints=True)
def etl_web_to_gcs_parent(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    total = 0
    for month in months:
        rows = etl_web_to_gcs(month, year, color)
        total += rows

    print(f"Total processed data {total}")


if __name__ == "__main__":
    months = [2, 3]
    year = 2019
    color = "yellow"
    etl_web_to_gcs_parent(months, year, color)
