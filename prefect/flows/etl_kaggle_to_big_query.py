import logging
import os
import shutil
import subprocess
import zipfile
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi
from prefect import flow, task
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse
from prefect_gcp.cloud_storage import GcsBucket


BASE_DIR = './prefect/data/fifa/'
DATASET_NAME = 'fifa-23-complete-player-dataset'
DATAPROC_CLUSTER = os.environ['DATAPROC_CLUSTER']
DATAPROC_REGION = os.environ['PROJECT_REGION']
PROJECT_ID = os.environ['PROJECT_ID']


@task
def download_kaggle_dataset() -> None:
    """
    Download dataset to be used from kaggle.
    It comes in a zip file containing various files, but we only care for male_players and male_teams in this example
    """
    if os.path.exists(BASE_DIR):
        logging.info('File already on disk!')
        return
    else:
        os.makedirs(BASE_DIR)

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        f'stefanoleone992/{DATASET_NAME}',
        path=BASE_DIR
    )

    path = Path(f'{BASE_DIR}{DATASET_NAME}.zip')

    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(BASE_DIR)


@task
def delete_local_files() -> None:
    if os.path.exists(BASE_DIR):
        shutil.rmtree(BASE_DIR)
    else:
        logging.info("Nothing to do...")


@task()
def write_to_gcs(datafile_type: str) -> None:
    """Upload local csv file to GCS"""
    gcs_block = GcsBucket.load("fifa-gcs")
    gcs_block.upload_from_path(
        from_path=f"{BASE_DIR}male_{datafile_type}.csv", to_path=f"raw/{datafile_type}.csv"
    )
    return


@task()
def create_external_table_teams() -> None:
    """Create external table for teams in BigQuery based on .csv file in bucket"""
    gcp_credentials = GcpCredentials.load("fifa-gcp-creds")
    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            f"""
            CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.fifa_data_all.external_fifa_teams`
            OPTIONS (
                format = 'CSV',
                uris = ['gs://dtc_data_lake_{PROJECT_ID}/raw/teams.csv']
            );
            """
        )


@task()
def create_materialized_table_teams() -> None:
    """Create materialized table based on external table for team data"""
    gcp_credentials = GcpCredentials.load("fifa-gcp-creds")
    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            f"""
            CREATE OR REPLACE TABLE {PROJECT_ID}.fifa_data_all.fifa_teams
            PARTITION BY fifa_update_date
            CLUSTER BY team_id, fifa_version, fifa_update, league_id AS
            SELECT 
                cast(team_id as integer) as team_id,
                cast(fifa_version as integer) as fifa_version, 
                cast(fifa_update as integer) as fifa_update, 
                cast(fifa_update_date as date) as fifa_update_date,
                cast(team_name as string) as team_name, 
                cast(league_id as integer) as league_id, 
                cast(league_name as string) as league_name, 
                cast(league_level as integer) as league_level, 
                cast(nationality_id as integer) as nationality_id, 
                cast(nationality_name as string) as nationality_name, 
                cast(overall as integer) as overall, 
                cast(attack as integer) as attack, 
                cast(midfield as integer) as midfield, 
                cast(defence as integer) as defence,
                cast(international_prestige as integer) as international_prestige, 
                cast(domestic_prestige as integer) as domestic_prestige
            FROM {PROJECT_ID}.fifa_data_all.external_fifa_teams
            WHERE league_id != 78;
            """
        )


@task()
def create_external_table_players() -> None:
    """Create external table for players in BigQuery based on .csv file in bucket"""
    gcp_credentials = GcpCredentials.load("fifa-gcp-creds")
    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            f"""
            CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.fifa_data_all.external_fifa_players`
            OPTIONS (
                format = 'CSV',
                uris = ['gs://dtc_data_lake_{PROJECT_ID}/raw/players.csv']
            );
            """
        )


@task()
def create_materialized_table_players() -> None:
    """Create materialized table based on external table for player data"""
    gcp_credentials = GcpCredentials.load("fifa-gcp-creds")
    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            f"""
            CREATE OR REPLACE TABLE {PROJECT_ID}.fifa_data_all.fifa_players
            PARTITION BY fifa_update_date
            CLUSTER BY player_id, fifa_version, fifa_update, club_team_id AS
            SELECT 
                cast(player_id as integer) as player_id, 
                cast(fifa_version as integer) as fifa_version, 
                cast(fifa_update as integer) as fifa_update, 
                cast(fifa_update_date as date) as fifa_update_date,
                cast(short_name as string) as short_name, 
                cast(overall as integer) as overall, 
                cast(potential as integer) as potential, 
                cast(value_eur as integer) as value_eur, 
                cast(wage_eur as integer) as wage_eur,
                cast(age as integer) as age, 
                cast(dob as date) as dob, 
                cast(height_cm as integer) as height_cm, 
                cast(weight_kg as integer) as weight_kg,
                cast(club_team_id as integer) as club_team_id, 
                cast(club_position as string) as club_position, 
                cast(nationality_id as integer) as nationality_id, 
                cast(nationality_name as string) as nationality_name,
                cast(preferred_foot as string) as preferred_foot,
                cast(international_reputation as integer) as international_reputation,
            FROM {PROJECT_ID}.fifa_data_all.external_fifa_players
            WHERE player_id is not null;
            """
        )


@flow
def process_fifa_teams() -> None:
    """Process team data, from .csv file to materialized table"""
    write_to_gcs("teams")
    create_external_table_teams()
    create_materialized_table_teams()


@flow
def process_fifa_players() -> None:
    """Process player data, from .csv file to materialized table"""
    write_to_gcs("players")
    create_external_table_players()
    create_materialized_table_players()


@task
def upload_spark_job(file_name: str) -> None:
    """Upload spark job to cloud bucket, to be run by dataproc"""
    gcs_block = GcsBucket.load("fifa-gcs")
    gcs_block.upload_from_path(
        from_path=f"./prefect/spark/{file_name}", to_path=f"spark/{file_name}"
    )
    return


@task()
def trigger_spark_job(file_name: str) -> None:
    """Trigger dataproc to run our pyspark job. The end result should be a table in bigquery ready to be visualized"""
    result = subprocess.run(
        [
            "gcloud", "dataproc", "jobs", "submit", "pyspark",
            f"--cluster={DATAPROC_CLUSTER}", f"--region={DATAPROC_REGION}",
            "--jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar",
            f"gs://dtc_data_lake_{PROJECT_ID}/spark/{file_name}", "--",
            f"--project_id={PROJECT_ID}"
        ]
    )
    logging.info(f"Command return code: {result.returncode} - Hopefully it is 0!")


@flow()
def etl_general_fifa_flow() -> None:
    """
    The main ETL function.
    It will download FIFA player and team data from Kaggle.
    The file is unzip and the player and team .csv files are directly uploaded to a google bucket.
    Then external and materialized tables are built in BigQuery based on these data.
    Local files are deleted.
    A Spark job that joins the tables in order to get aggregated team metrics over the years is uploaded to Dataproc.
    That job is run.
    In the end, the data is ready to be visualized.
    """
    download_kaggle_dataset()
    process_fifa_teams()
    process_fifa_players()
    delete_local_files()
    spark_job_file_name = "fifa_spark.py"
    upload_spark_job(spark_job_file_name)
    trigger_spark_job(spark_job_file_name)


if __name__ == "__main__":
    etl_general_fifa_flow()
