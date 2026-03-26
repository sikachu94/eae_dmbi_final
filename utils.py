"""
utils.py
--------
EAE - Data Management for BI | Final Assignment
Author: Omar Ahmed

Description:
    Helper functions used by the ETL pipeline (load_csv_to_mysql.py).
    Split into two sections
        - Database helpers: engine creation, read, write  from mysql server
        - Cleaning functions used by the python pipeline (load_csv_to_mysql.py).
            - 1 function per table
"""

import numpy as np
import pandas as pd
import mysql.connector
from mysql.connector.connection import MySQLConnection
from pathlib import Path


# ---------------------------------------------------------------------------
# Database Helper Functions
# ---------------------------------------------------------------------------

def get_db_connection(creds: dict) -> MySQLConnection:
    """
    Creates and returns a mysql.connector connection from a credentials dictionary.

    Args:
        creds (dict): Must contain keys: username, password, host, database.

    Returns:
        MySQLConnection: An open connection to the MySQL database.
    """
    connection = mysql.connector.connect(
        host=creds["HOST"],
        user=creds["USERNAME"],
        password=creds["PASSWORD"],
        database=creds["DATABASE"],
    )
    return connection

def read_from_database(connection: MySQLConnection, query: str) -> pd.DataFrame:
    """
    Executes a SQL query and returns the result as a pandas DataFrame.
 
    Args:
        connection (MySQLConnection): Active mysql.connector connection.
        query      (str):             SQL query to execute.
 
    Returns:
        pd.DataFrame: Query results.
    """
    cursor = connection.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(rows, columns=columns)
 
def write_to_database(
    connection: MySQLConnection,
    df: pd.DataFrame,
    table_name: str,
    primary_keys: list = None,
    batch_size: int = 5000,
) -> None:
    cursor = connection.cursor()
 
    # --- 1. Check if the table already exists ---
    cursor.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (table_name,)
    )
    table_exists = cursor.fetchone()[0] > 0
 
    rows_to_insert = df

    if not table_exists:
        # --- 2. CREATE TABLE LOGIC ---
        print(f"  [LOAD] Table '{table_name}' not found — creating...")
        col_definitions = []
        for col, dtype in zip(df.columns, df.dtypes):
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "BIGINT"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "DOUBLE"
            elif pd.api.types.is_datetime64_any_dtype(dtype) or col == 'date':
                sql_type = "DATE"
            else:
                sql_type = "VARCHAR(255)"
            col_definitions.append(f"`{col}` {sql_type}")
 
        if primary_keys:
            pk_cols = ", ".join([f"`{k}`" for k in primary_keys])
            col_definitions.append(f"PRIMARY KEY ({pk_cols})")
 
        create_sql = f"CREATE TABLE `{table_name}` ({', '.join(col_definitions)})"
        cursor.execute(create_sql)
        insert_sql_template = "INSERT INTO"
    else:
        # --- 3. DUPLICATE CHECK LOGIC ---
        print(f"  [LOAD] Table '{table_name}' exists — checking for new records...")
        if primary_keys:
            pk_cols_str = ", ".join([f"`{k}`" for k in primary_keys])
            cursor.execute(f"SELECT {pk_cols_str} FROM `{table_name}`")
            existing_pks = cursor.fetchall()

            existing_set = {tuple(str(val) for val in row) for row in existing_pks}
            df_pk_tuples = list(map(tuple, df[primary_keys].astype(str).to_numpy()))
            mask = [t in existing_set for t in df_pk_tuples]  # plain list of bools
            rows_to_insert = df[~pd.Series(mask, index=df.index)]

            print(f"  [QC] Found {len(df) - len(rows_to_insert)} existing rows. "
                  f"New rows to process: {len(rows_to_insert)}")
        
        insert_sql_template = "INSERT IGNORE INTO"
 
    # --- 4. DATA LOADING ---
    if rows_to_insert.empty:
        print(f"  [LOAD] No new data to write for '{table_name}'.")
        cursor.close()
        return

    col_names = ", ".join([f"`{col}`" for col in rows_to_insert.columns])
    placeholders = ", ".join(["%s"] * len(rows_to_insert.columns))
    insert_sql = f"{insert_sql_template} `{table_name}` ({col_names}) VALUES ({placeholders})"
 
    rows = [
        tuple(None if pd.isnull(val) else val for val in row)
        for row in rows_to_insert.itertuples(index=False, name=None)
    ]
 
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        cursor.executemany(insert_sql, batch)
        connection.commit()
 
    cursor.close()

def load_sql_file(filepath: Path) -> str:
    """
    Reads a .sql file from disk and returns its content as a string.
 
    Args:
        filepath (Path): Path to the .sql file.
 
    Returns:
        str: The SQL content of the file.
    """
    with open(filepath, "r", encoding="utf-8") as file:
        return file.read()
 
 
    """
    Reads a .sql file from disk and returns its content as a string.

    Args:
        filepath (Path): Path to the .sql file.

    Returns:
        str: The SQL content of the file.
    """
    with open(filepath, "r", encoding="utf-8") as file:
        return file.read()


# ---------------------------------------------------------------------------
# Data Cleaning Functions
# ---------------------------------------------------------------------------
# Each function below corresponds to one source table.
# They receive a raw DataFrame and return a cleaned one.
# Cleaning operations are documented inline.
# ---------------------------------------------------------------------------

def clean_sales_funnel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and validates the raw sales funnel DataFrame (sale_phases_funnel.csv).

    Cleaning operations applied:
        1. Standardise column names to lowercase.
        2. Remove duplicate leads (same lead_id appearing more than once).
        3. Impute missing installation_peak_power_kw using n_panels × a fixed
           kW-per-panel constant (derived from complete rows). Justified by the
           the business selling one unique panel type. Drop rows where
           n_panels is also null (impossible to compute). Drop rows where
           installation_price is null — cannot be computed because installation
           hours/complexity data is not available in the dataset.
        4. Validate categorical fields financing_type and visiting_company
           against the expected values defined as expected in the business context.
           Unexpected values are set to null rather than dropped,
           so the lead is still counted in volume KPIs.
        5. Parse all date columns to proper datetime types so MySQL stores
           them as DATE
        6. Strip leading/trailing whitespace from string columns to avoid
           silent mismatches in GROUP BY and JOIN operations.

    Notes on intentional nulls (not treated as dirty data):
        - contract_2_dispatch_date / contract_2_signature_date: null for all
          cash leads, as the second contract only exists for financed sales
          (assignment page 2).
        - ko_date / ko_reason: null for leads still in the funnel. ko_reason
          is also only stored for some KO leads (assignment pages 2-3).
        - sale_dismissal_date: null for all leads that did not cancel within
          15 days of purchase (assignment page 2).

    Args:
        df (pd.DataFrame): Raw sales funnel data.

    Returns:
        pd.DataFrame: Cleaned sales funnel data.
    """

    # --- 1. Standardise column names ---
    df.columns = df.columns.str.strip().str.lower()

    # --- 2. Remove duplicate lead_id rows ---
    rows_before = len(df)
    df = df.drop_duplicates(subset=["lead_id"])
    duplicates_removed = rows_before - len(df)
    if duplicates_removed > 0:
        print(f"  [QC] Removed {duplicates_removed} duplicate lead_id rows.")

    # --- 3. Calculate missing installation_peak_power_kw where possible,
    #        then fill installation_peak_power_kw where n_panels is null with 0.


    # Derive the fixed kW-per-panel constant from complete rows
    complete_mask = (
        df["installation_peak_power_kw"].notna()
        & df["n_panels"].notna()
        & (df["n_panels"] > 0)
    )
    avg_kw_per_panel = (
        df.loc[complete_mask, "installation_peak_power_kw"]
        / df.loc[complete_mask, "n_panels"]
    ).mean()
    print(f"  [QC] Derived panel constant: {avg_kw_per_panel:.4f} kW/panel")

    # Impute peak_power_kw using n_panels × avg_kw_per_panel
    mask_kw = df["installation_peak_power_kw"].isna() & df["n_panels"].notna()
    df.loc[mask_kw, "installation_peak_power_kw"] = (
        df.loc[mask_kw, "n_panels"] * avg_kw_per_panel
    )
    if mask_kw.sum() > 0:
        print(f"  [QC] Imputed {mask_kw.sum()} missing peak_power_kw values from n_panels.")

    # Fill rows where peak_power_kw is still null with 0 (because n_panels is null)
    rows_before = len(df)
    df["installation_peak_power_kw"] = df["installation_peak_power_kw"].fillna(0)

    # --- 4. Validate categorical fields against expected business values ---

    # customers pay either by cash or by financing. 
    # Any other value is unexpected and set to null.   
    if "financing_type" in df.columns:
        valid_financing = {"cash", "financed"}
        unexpected_financing = ~df["financing_type"].str.lower().isin(valid_financing) \
                               & df["financing_type"].notna()
        if unexpected_financing.sum() > 0:
            print(f"  [QC] Found {unexpected_financing.sum()} unexpected financing_type values "
                  f"— setting to null: {df.loc[unexpected_financing, 'financing_type'].unique()}")
            df.loc[unexpected_financing, "financing_type"] = None

    # Installations are carried out by either an internal or external team. 
    # Any other value is unexpected.
    if "visiting_company" in df.columns:
        valid_visiting = {"internal", "external"}
        unexpected_visiting = ~df["visiting_company"].str.lower().isin(valid_visiting) \
                              & df["visiting_company"].notna()
        if unexpected_visiting.sum() > 0:
            print(f"  [QC] Found {unexpected_visiting.sum()} unexpected visiting_company values "
                  f"— setting to null: {df.loc[unexpected_visiting, 'visiting_company'].unique()}")
            df.loc[unexpected_visiting, "visiting_company"] = None

    # --- 5. Parse date columns to datetime ---

    date_columns = [
        "offer_sent_date",
        "contract_1_dispatch_date",
        "contract_2_dispatch_date",
        "contract_1_signature_date",
        "contract_2_signature_date",
        "visit_date",
        "technical_review_date",
        "project_validation_date",
        "sale_dismissal_date",
        "ko_date",
    ]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    # --- 6. Strip whitespace from string/object columns ---
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    return df


def clean_zipcode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the zip code dimension table (zipcode_eae.csv).

    Cleaning operations applied:
        1. Standardise column names to lowercase.
        2. Remove rows where ZIPCODE is null – it is the primary key.
        3. Remove duplicate zip codes (keep first occurrence).
        4. Strip whitespace from string columns.

    Args:
        df (pd.DataFrame): Raw zip code data.

    Returns:
        pd.DataFrame: Cleaned zip code data.
    """

    # --- 1. Standardise column names ---
    df.columns = df.columns.str.strip().str.lower()

    # --- 2. Drop rows with null zipcode (PK – cannot be null) ---
    rows_before = len(df)
    df = df.dropna(subset=["zipcode"])
    if len(df) < rows_before:
        print(f"  [QC] Removed {rows_before - len(df)} rows with null zipcode.")

    # --- 3. Remove duplicate zip codes ---
    rows_before = len(df)
    df = df.drop_duplicates(subset=["zipcode"])
    if len(df) < rows_before:
        print(f"  [QC] Removed {rows_before - len(df)} duplicate zipcodes.")

    # --- 4. Strip whitespace from string columns ---
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    return df


def clean_weather(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the weather dimension table (meteo_eae.csv).

    Cleaning operations applied:
        1. Standardise column names to lowercase.
        2. Parse the date column to proper datetime type.
        3. Remove rows where both zipcode and date are null
           (cannot be joined to fact table without these keys).
        4. Remove exact duplicate rows (same zipcode + date + all metrics).

    Args:
        df (pd.DataFrame): Raw weather data.

    Returns:
        pd.DataFrame: Cleaned weather data.
    """

    # --- 1. Standardise column names ---
    df.columns = df.columns.str.strip().str.lower()

    # --- 2. Parse date column with explicit format ---
    #
    if "date" in df.columns:
        # Format in the CSV
        DATE_FORMAT = "%Y/%m/%d %H:%M:%S.%f"

        df["date"] = pd.to_datetime(df["date"], format=DATE_FORMAT, errors="coerce")
        df["date"] = df["date"].dt.normalize() # Keeps it as datetime64 but sets time to 00:00:00

        nat_count = df["date"].isna().sum()
        if nat_count > 0:
            print(f"  [QC] WARNING: {nat_count} date values could not be parsed with "
                  f"format '{DATE_FORMAT}'. Check the raw sample above and update DATE_FORMAT.")

    # --- 3. Drop rows missing the join keys ---
    join_keys = [col for col in ["zipcode", "date"] if col in df.columns]
    rows_before = len(df)
    df = df.dropna(subset=join_keys)
    if len(df) < rows_before:
        print(f"  [QC] Removed {rows_before - len(df)} rows missing join keys in weather.")

    # --- 4. Remove exact duplicates ---
    rows_before = len(df)
    df = df.drop_duplicates()
    if len(df) < rows_before:
        print(f"  [QC] Removed {rows_before - len(df)} duplicate rows in weather.")

    return df