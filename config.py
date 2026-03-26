"""
config.py
--------
EAE - Data Management for BI | Final Assignment
Author: Omar Ahmed

Description:
    - all constants utilized by the load_csv_to_mysql.py
"""

from pathlib import Path
from utils import (
    clean_sales_funnel,
    clean_zipcode,
    clean_weather
)

# Path to the credentials file
CREDS_PATH = Path("C:/Users/HP/Desktop/Omar/masters/DM BI/assignment4_final/creds.yml")

# Folder where the raw CSV files live
DATA_DIR = Path("C:/Users/HP/Desktop/Omar/masters/DM BI/assignment4_final/data")

# Naming convention:
#   spf_ prefix _fact suffix for sales fact tables
#   spf_ prefix _dim suffix  for sales dimension tables

sales = "spf_sales_fact"
zipcode =  "spf_zipcode_dim"
weather = "spf_weather_dim"


# Mapping CSV filename to target MySQL table name
CSV_TO_TABLE = {
    "sale_phases_funnel.csv": sales,
    "zipcode_eae.csv":      zipcode ,
    "meteo_eae.csv":        weather,
}

# Handle varying csv separators
CSV_SEPARATOR = {
    "sale_phases_funnel.csv": ";",
    "zipcode_eae.csv":        ",",
    "meteo_eae.csv":        ";",
}

# Cleaning functions to apply per table (imported from utils.py)
CLEANING_FUNCTIONS = {
    sales:   clean_sales_funnel,
    zipcode:  clean_zipcode,
    weather:  clean_weather,
}

# Primary keys per table
# on first run: to create primary key
# on subsequent runs: enable incremental load
PRIMARY_KEYS = {
    sales:  ["lead_id"],
    zipcode: ["zipcode"],
    weather: ["zipcode", "date"],
}
