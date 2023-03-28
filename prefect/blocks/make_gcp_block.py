from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# alternative to creating GCP blocks in the UI
# insert your own service_account_file path or service_account_info dictionary from the json file
# IMPORTANT - do not store credentials in a publicly available repository!


credentials_block = GcpCredentials(
    service_account_info={}  # enter your credentials info or use the file method.
)
credentials_block.save("fifa-gcp-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("fifagcp-creds"),
    bucket="fifa_data_all",  # insert your  GCS bucket name
)

bucket_block.save("fifa-gcs", overwrite=True)
