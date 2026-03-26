"""
load_csv_to_mysql.py
--------------------
EAE - Data Management for BI | Final Assignment
Author: Omar Ahmed

Description:
    Main ETL pipeline script. Reads the raw CSV files (sales funnel, zipcodes,
    meteo weather) from a local directory, applies data cleaning and quality checks,
    and loads the cleaned data into a MySQL database following the star schema
    defined for this project.

    The pipeline follows an ETL approach:
        1. Extract  – read CSVs into pandas DataFrames
        2. Transform – clean and validate the data in Python
        3. Load      – write the final tables to MySQL

Usage:
    Update CREDS_PATH and DATA_DIR to match your local setup, then run:
        python load_csv_to_mysql.py
"""

import yaml
import pandas as pd

from utils import (
    get_db_connection,
    write_to_database,
)

from config import (
    CREDS_PATH,
    DATA_DIR,
    CSV_TO_TABLE,
    CSV_SEPARATOR,
    CLEANING_FUNCTIONS,
    PRIMARY_KEYS
)

def connect_to_database(CREDS_PATH):
    """
    Load database using creds

    Args:
        CREDS_PATH (Path): Path to creds.yml file
        
    Returns:
        MySQLConnection: An open connection to the data warehouse
    """
    # 1. Load database credentials from YAML file
    print("Loading credentials...")
    with open(CREDS_PATH, "r") as f:
        creds = yaml.safe_load(f)
 
    # 2. Create mysql.connector connection targeting the configured schema
    db_connection = get_db_connection(creds["datawarehouse"])
    print("Database connection established.\n")
    return db_connection

def extract_data(csv):
    """
    clean data

    Args:
        csv (str): name of csv file (make sure to include .csv suffix - check config.py file)
        
    Returns:
        df (DataFrame): raw data DataFrame (to pass to clean_data function)
    """
    table_name = CSV_TO_TABLE[csv]
    csv_path = DATA_DIR / csv
    print(f"[EXTRACT] Reading {csv} ...")

    # Read CSV using the correct separator for each file
    sep = CSV_SEPARATOR.get(csv, ",")
    df = pd.read_csv(csv_path, sep=sep)
    print(f"  Raw rows loaded: {len(df)}")
    return df

def transform_data(df, table_name):
    
    """
    run data transformations and cleaning functions

    Args:
        df (DataFrame): a dictionary with the table name key and raw data DataFrame value
        table_name (str): name of the table in database
        
    Returns:
        clean_df (DataFrame): clean dataframe (to pass to process_data function)
    """
    # Apply the cleaning function for each table
    cleaning_fn = CLEANING_FUNCTIONS.get(table_name)
    if cleaning_fn:
        print(f"[TRANSFORM] Cleaning {table_name} ...")
        clean_df = cleaning_fn(df)
        print(f"  Rows after cleaning: {len(clean_df)}")
        return clean_df
 

def load_data(db_connection, df, table_name):
    """
    load data to database

    Args:
        db_connection (MySQLConnection): connection engine to local mysql server
        df (DataFrame): cleaned dataframe to be loaded to database
        table_name (str): name of table as it will appear in database
        
    Returns:
        No return - writes directly to database and executes write_to_database function
    """
    # Write to MySQL — write_to_database auto-detects whether the table
    # exists and handles first run (full create + insert) vs subsequent
    # runs (INSERT IGNORE, skipping already-loaded rows) automatically.
    pks = PRIMARY_KEYS.get(table_name)
    print(f"[LOAD] Writing to table '{table_name}' ...")
    write_to_database(db_connection, df, table_name, primary_keys=pks)
    print(f"  Done. {len(df)} rows processed for '{table_name}'.\n")

def execute_ETL():
    """
    Orchestrates the full ETL pipeline:
        1. Load credentials and create DB engine
        2. For each CSV: read -> clean -> load into MySQL
    
    Args:
        db_connection (MySQLConnection): connection engine to local mysql server
        df (DataFrame): cleaned dataframe to be loaded to database
        table_name (str): name of table as it will appear in database
        
    Returns:
        No return - writes directly to database and executes write_to_database function
    """
    
    db_connection = connect_to_database(CREDS_PATH) # create db connection

    for csv, table_name in CSV_TO_TABLE.items(): # for each data file (as specified in config.py)
        df = extract_data(csv) # create raw data dataframes
        clean_df = transform_data(df, table_name) # transform and clean data according to table cleaning function mapping
        load_data(db_connection, clean_df, table_name) # load data to database
    

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main():
    execute_ETL()
    print("All Pipelines completed successfully.")
 
 
if __name__ == "__main__":
    main()