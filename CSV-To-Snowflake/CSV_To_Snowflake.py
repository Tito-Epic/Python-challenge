# Script to read CSV files in a directory and ingest each file into Snowflake

import os
import csv
import logging
import snowflake.connector
from typing import Dict, List, Tuple

# Set up logging with a specific format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_directory(directory: str, table_name: str, stage_name: str) -> None:
    """
    Load all CSV files from a directory into a Snowflake stage table using parallel load.

    Args:
        directory (str): Path to the directory containing CSV files.
        table_name (str): Name of the Snowflake target table.
        stage_name (str): Name of the Snowflake stage.
    """
    # Find all CSV files in the directory
    csv_files = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith(".csv")]

    # Load each CSV file into the stage table
    for csv_file in csv_files:
        load_csv_to_snowflake(csv_file, table_name, stage_name)

def get_column_names(csv_file: str) -> Tuple[List[str]]:
    """
    Get column names and infer data types from a CSV file.

    Args:
        csv_file (str): Path to the CSV file.

    Returns:
        Tuple[List[str]]: A tuple containing List of column names

    """
    column_names = []

    with open(csv_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        column_names = next(reader)  # Get the first row as column names

 

    return column_names 

def load_csv_to_snowflake(csv_file: str, table_name: str, stage_name: str) -> None:
    """
    Load data from a CSV file into a Snowflake table using parallel load.

    Args:
        csv_file (str): Path to the CSV file.
        table_name (str): Name of the Snowflake target table.
        stage_name (str): Name of the Snowflake stage.
    """
    # Connect to Snowflake using environment variables
    ctx = snowflake.connector.connect(
        account=os.environ.get("SF_ACCOUNT"),
        user=os.environ.get("SF_USER"),
        password=os.environ.get("SF_PASSWORD"),
        warehouse=os.environ.get("SF_WAREHOUSE"),
        database=os.environ.get("SF_DATABASE"),
        schema=os.environ.get("SF_SCHEMA"),
        role=os.environ.get("SF_ROLE")
    )
    cs = ctx.cursor()

    # Get column names and data types from the CSV file
    column_names = get_column_names(csv_file)

    # Stage the CSV file in Snowflake
    put_command = f"put file://{csv_file} @{stage_name} AUTO_COMPRESS = FALSE PARALLEL = 2"

    # Excute the put SQL statment and 
    cs.execute(put_command)

    logging.info(f"File '{csv_file}' staged successfully.")

    # Prepare column list for the COPY INTO command
    column_list = ','.join(f'"{col}"' for col in column_names)

    # Load data from the staged file into the target table
    copy_command = f"COPY INTO {table_name} ({column_list}) FROM @{stage_name}/{os.path.basename(csv_file)} FILE_FORMAT = (FORMAT_NAME = csvformat) ON_ERROR = 'CONTINUE'"
    cs.execute(copy_command)
    ctx.commit()
    logging.info(f"Data from '{csv_file}' loaded into '{table_name}' successfully.")

    # Close the Snowflake connection and session 
    cs.close()
    ctx.close()

if __name__ == "__main__":
    csv_directory = "/pth/sup_pth"
    table_name = "target_table_name"
    stage_name = "internal_stage_name"