from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
import os


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url: str) -> pd.DataFrame:
    """Read taxi data from web to pandas Dataframe"""
    
    df = pd.read_csv(url)
    return df

@task(log_prints=True)
def clean(df :pd.DataFrame) -> pd.DataFrame:
    """Fix some dtypes issues"""

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df : pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write dataframe out locally as a parquet file"""

    outdir = Path(f'data/{color}')
    if not os.path.exists(outdir):
        outdir.mkdir(parents=True, exist_ok=True)

    path = os.path.join(outdir, Path(f"{dataset_file}.parquet"))
    print(path)
    print(os.getcwd())
    df.to_parquet(path, compression="gzip", )
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload a local file to gcs"""

    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path = path, to_path=path)
    return

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(year: int = 2021, months: list[int] = [1, 2], color: str = "yellow"):
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == '__main__':
    months = [1, 2, 3]
    etl_parent_flow(months=months)
