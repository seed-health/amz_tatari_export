import os
from dotenv import load_dotenv
import logging
import boto3
import snowflake.connector
from boto3.s3.transfer import S3Transfer
import csv
from utilities import *
from io import StringIO
import pandas as pd
from datetime import datetime as dt
from botocore.exceptions import NoCredentialsError
import glob

# Load environment variables and set up paths and filenames
FILE_PATH = os.path.abspath(os.path.join(__file__, "../.."))
LOG_DIRECTORY = f"{FILE_PATH}/logs"
LOG_FILE = os.environ.get("LOGGING_FILE")
LOG_FILE_PATH = os.path.join(LOG_DIRECTORY, LOG_FILE)
LOGGING_HEADER = "AMZ TATARI DATA EXPORT"

AMZ_S3_FILE_NAME = f"{FILE_PATH}/amz_data/amz_tatari_export_{dt.now().strftime('%Y-%m-%d_%H-%M')}.csv"
TARGET_S3_FILE_NAME = f"{FILE_PATH}/target_data/tatari_export_{dt.now().strftime('%Y-%m-%d_%H-%M')}.csv"

SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL")

QUERY_DIRECTORY = f"{FILE_PATH}/queries"  # Directory where SQL files are stored
QUERY_FILE = "amz_query.sql"  # SQL file to be executed
DB_NAME = os.environ.get("SF_DATABASE")
DB_USER = os.environ.get("SF_USER")
DB_PASSWORD = os.environ.get("SF_PASSWORD")
DB_ACCOUNT = os.environ.get("SF_ACCOUNT")
DB_WAREHOUSE = os.environ.get("SF_WAREHOUSE")
DB_ROLE = os.environ.get("SF_ROLE")

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
# S3_BUCKET = os.environ.get("S3_BUCKET")
S3_BUCKET_ARN = "arn:aws:s3:::tatari-partners-us-east-1"
S3_PREFIX = "seed/"

# Initialize logging
os.makedirs(LOG_DIRECTORY, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

def load_environment_variables():
    """
    Load environment variables from the .env file.
    """
    file_path = os.path.abspath(os.path.join(__file__, "../.."))
    env_path = f"{file_path}/src/.env"
    load_dotenv(dotenv_path=env_path)

    logger.info("Environment variables loaded")


def setup_aws_credentials():
    """
    Set up AWS credentials using environment variables.
    """
    return {
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
    }

def setup_aws_session():
    """
    Set up AWS session using credentials for dev_admin.
    """
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1",  # specify the region if necessary
    )
    return session.client("s3")


def setup_snowflake_credentials():
    """
    Set up Snowflake credentials using environment variables.
    """
    return {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "role": DB_ROLE,
        "account": DB_ACCOUNT,
        "warehouse": DB_WAREHOUSE,
    }

def connect_to_snowflake(credentials):
    """
    Establish a connection to Snowflake using provided credentials.
    """
    return snowflake.connector.connect(**credentials)



def upload_csv_files_to_s3(data_folder, aws_credentials, S3_PREFIX):
    """
    Upload all CSV files from a local folder to an S3 bucket.

    Parameters:
    data_folder (str): The local directory containing CSV files.
    aws_credentials (dict): AWS credentials for accessing S3.
    """
    try:
        # Define the bucket name and prefix from the ARN
        S3_BUCKET = "tatari-exports"
        SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL")
        SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")
        # Initialize the S3 client
        s3 = boto3.client("s3", **aws_credentials)
        
        # Walk through the data folder to find all CSV files
        for root, dirs, files in os.walk(data_folder):
            for file in files:
                if file.endswith(".csv"):
                    local_file_path = os.path.join(root, file)
                    s3_file_name = os.path.join(S3_PREFIX, file)

                    logger.info(f"Uploading {local_file_path} to S3 as {s3_file_name}")

                    with open(local_file_path, "rb") as file_data:
                        s3.put_object(
                            Bucket=S3_BUCKET,
                            Key=s3_file_name,
                            Body=file_data
                        )

                    logger.info(f"File uploaded successfully to {S3_BUCKET}/{s3_file_name}")
                    slack_notification(SLACK_CHANNEL, SLACK_API_TOKEN, f":tatari1: {s3_file_name} loaded successfully to S3 :white_check_mark:")

    
    except Exception as e:
        logger.error(f"Failed to upload files to S3: {e}")

def read_sql_file(file_path):
    """
    Read a SQL query from a file.
    
    Parameters:
    file_path (str): The path to the .sql file.
    
    Returns:
    str: The content of the SQL file as a string.
    """
    with open(file_path, 'r') as file:
        query = file.read()
    return query

def pull_data_from_snowflake(snowflake_connection, query_file, file_name):
    query_file = os.path.abspath(os.path.join(QUERY_DIRECTORY, query_file))
    """
    Query data from Snowflake and save it to a CSV file.

    Parameters:
    snowflake_connection: The Snowflake connection object.
    
    Returns:
    str: The path to the saved CSV file.
    """
    with snowflake_connection.cursor() as cur:
        cur.execute(f"USE WAREHOUSE {DB_WAREHOUSE}")
        cur.execute(f"USE ROLE {DB_ROLE}")
        cur.execute(f"USE DATABASE {DB_NAME}")

        cur.execute(
            f"""    
            {read_sql_file(query_file)}
            """
        )
        data = cur.fetchall()
        # Log the row count
        row_count = len(data)
        logging.info(f"Number of rows fetched: {row_count}")
        file_name = f"{file_name}"
        with open(file_name, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow([desc[0] for desc in cur.description])  # Write header row
            writer.writerows(data)
        return file_name

def delete_old_files(file_path, pattern="*.csv"):
    """
    Delete files matching a wildcard pattern in the specified directory.

    Parameters:
    file_path (str): The directory path where files are located.
    pattern (str): The wildcard pattern for matching files (default is '*.csv').
    """
    # Construct the full file path pattern
    search_pattern = os.path.join(file_path, pattern)

    # List all files matching the pattern
    for file in glob.glob(search_pattern):
        # Check if it's a file
        if os.path.isfile(file):
            try:
                os.remove(file)
                print(f"Deleted {file}")
            except Exception as e:
                print(f"Error deleting {file}: {e}")


if __name__ == "__main__":
    logger = setup_logging(LOG_FILE)
    logger.info(f"{LOGGING_HEADER} report data load begins at {str(dt.now())}...")
    logger.info(f"Impact report data load begins at {str(dt.now())}...")

    load_environment_variables()
    logger.info("Script executed successfully")
    delete_old_files(f"{FILE_PATH}/amz_data", pattern="*.csv")
    delete_old_files(f"{FILE_PATH}/target_data", pattern="*.csv")
    aws_credentials = setup_aws_credentials()
    aws_session = setup_aws_session()
    snowflake_credentials = setup_snowflake_credentials()
    snowflake_connection = connect_to_snowflake(snowflake_credentials)

    amz_data_table = pull_data_from_snowflake(snowflake_connection, 'amz_pull.sql', AMZ_S3_FILE_NAME)
    target_data_table = pull_data_from_snowflake(snowflake_connection, 'target_pull.sql', TARGET_S3_FILE_NAME)
    try:
        upload_csv_files_to_s3(f"{FILE_PATH}/amz_data", aws_credentials, "seed/amazon/")
        upload_csv_files_to_s3(f"{FILE_PATH}/target_data", aws_credentials, "seed/target/")

    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        slack_notification(SLACK_CHANNEL, SLACK_API_TOKEN, f":tatari1: Error uploading file to S3: {e} :x:")
