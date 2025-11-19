# Databricks notebook source
import os
import json
import logging
import pandas as pd
from datetime import datetime
from typing import List, Tuple
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DateType, TimestampType, DoubleType
)

# Import the DataCurator class and helpers
from curation.data_curator import (
    DataCurator,
    Constants,
    logger,
    load_excel_mapping
)

# COMMAND ----------

# Configure logging
logging.basicConfig(level=logging.INFO)

# COMMAND ----------

# ========================================================================
# Configuration Loading
# ========================================================================

def load_config(config_path: str = "config.json") -> dict:
    """Load and validate configuration file."""
    try:
        with open(config_path) as f:
            config = json.load(f)

        required_keys = ["legacy_raw_bkt", "data_bkt", "raw_data_dir"]
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise ValueError(f"Missing required config keys: {missing_keys}")

        logger.info("Configuration loaded successfully")
        return config

    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        raise


env = os.environ.get('DATAENV')
logger.info(f"Environment: {env}")

config = load_config()

resolved_env = "prod" if env == "prd" else env

legacy_raw_bkt = config["legacy_raw_bkt"].format(env=env)
legacy_raw_bkt_mount_point = config["legacy_raw_bkt_mount_point"].rstrip("/")
ss_header_mapping_file_path = config["ss_header_mapping_file_path"].format(env=resolved_env)
treatment_group_mapping_file_path = config["treatment_group_mapping_file_path"].format(env=resolved_env)

data_bkt = config["data_bkt"].format(env=env)
data_bkt_mount_point = config["data_bkt_mount_point"].rstrip("/")

raw_data_dir = config["raw_data_dir"]

# Historical load controls
historical_load = config.get("historical_load", False)
start_date = config.get("start_date")
end_date = config.get("end_date")

# COMMAND ----------

# ========================================================================
# Mount S3 Buckets
# ========================================================================

def ensure_mount(mount_point: str, bucket_name: str):
    """Ensure S3 bucket is mounted."""
    if not any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        dbutils.fs.mount(source=f"s3a://{bucket_name}", mount_point=mount_point)
        logger.info(f"Mounted {bucket_name} at {mount_point}")
    else:
        logger.info(f"Mount already exists: {mount_point}")


ensure_mount(legacy_raw_bkt_mount_point, legacy_raw_bkt)
ensure_mount(data_bkt_mount_point, data_bkt)

# COMMAND ----------

# MAGIC %md
# MAGIC #### File Reading Helper Functions

# COMMAND ----------

def read_csv_with_dynamic_header(dbfs_path: str, max_rows: int = 10) -> pd.DataFrame:
    """
    Read CSV file from DBFS with dynamic header detection.

    Args:
        dbfs_path: DBFS path (e.g., "dbfs:/mnt/...")
        max_rows: Maximum rows to search for header

    Returns:
        pandas DataFrame
    """
    # Convert dbfs:/ path to /dbfs/ path for pandas
    local_path = dbfs_path.replace("dbfs:", "/dbfs")

    logger.info(f"Reading CSV: {dbfs_path}")

    with open(local_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= max_rows:
                raise ValueError(f"No valid header found in first {max_rows} rows")

            cells = [c.strip() for c in line.split(',')]

            # Check if all cells are non-empty and has multiple columns
            if len(cells) > 1 and all(cells):
                logger.debug(f"Found header at line {i}")
                df = pd.read_csv(local_path, dtype=str, encoding='utf-8', skiprows=i)
                logger.info(f"Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")
                return df

    raise ValueError(f"No fully populated header line found in {dbfs_path}")


def load_csv_files(file_list: List[Tuple[str, str]]) -> List[Tuple[pd.DataFrame, str]]:
    """
    Load multiple CSV files into DataFrames.

    Args:
        file_list: List of (file_path, file_name) tuples from dbutils

    Returns:
        List of (DataFrame, filename) tuples
    """
    dataframes = []

    for file_path, file_name in file_list:
        try:
            df = read_csv_with_dynamic_header(file_path)
            dataframes.append((df, file_name))
            logger.info(f"✓ Loaded {file_name}: {df.shape}")
        except Exception as e:
            logger.error(f"✗ Error loading {file_name}: {str(e)}")
            continue

    return dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize DataCurator

# COMMAND ----------

# Load mapping file
ss_header_mapping_file_path_full = f"/dbfs{os.path.join(legacy_raw_bkt_mount_point, ss_header_mapping_file_path)}"
ss_header_mapping_df = load_excel_mapping(ss_header_mapping_file_path_full)

# Initialize DataCurator with mapping
curator = DataCurator(mapping_df=ss_header_mapping_df)
logger.info("DataCurator initialized with mapping file")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Identify date folders to be processed

# COMMAND ----------

def get_available_date_folders(base_path: str):
    """Return sorted list of (folder_name, datetime_obj) tuples."""
    folders = []
    for f in dbutils.fs.ls(base_path):
        name = f.name.strip("/")
        if not name.isdigit():
            continue
        try:
            date_obj = datetime.strptime(name, Constants.DATE_FOLDER_FORMAT)
            folders.append((name, date_obj))
        except ValueError:
            continue
    return sorted(folders, key=lambda x: x[1])


raw_data_path = f"{data_bkt_mount_point}/{raw_data_dir}"
folders = get_available_date_folders(raw_data_path)

if not folders:
    raise Exception(f"No valid date folders found in {raw_data_path}")

# Determine folders to process
selected_folders = []

if historical_load:
    start_dt = datetime.strptime(start_date, Constants.DATE_FOLDER_FORMAT) if start_date else None
    end_dt = datetime.strptime(end_date, Constants.DATE_FOLDER_FORMAT) if end_date else None

    if not start_dt and not end_dt:
        selected_folders = [f for f, _ in folders]
        logger.info(f"Historical load enabled — processing ALL {len(selected_folders)} folders")
    elif start_dt and not end_dt:
        selected_folders = [f for f, d in folders if d >= start_dt]
        logger.info(f"Historical load from {start_date} onwards — {len(selected_folders)} folders")
    elif not start_dt and end_dt:
        selected_folders = [f for f, d in folders if d <= end_dt]
        logger.info(f"Historical load up to {end_date} — {len(selected_folders)} folders")
    else:
        selected_folders = [f for f, d in folders if start_dt <= d <= end_dt]
        logger.info(f"Historical load from {start_date} to {end_date} — {len(selected_folders)} folders")
else:
    latest_folder = folders[-1][0]
    selected_folders = [latest_folder]
    logger.info(f"Standard mode — processing latest folder only: {latest_folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### File Discovery Functions

# COMMAND ----------

def find_latest_summary_files(date_folder_path: str) -> dict:
    """
    For each study subfolder within a date folder, find the latest CSV file for each category.

    Args:
        date_folder_path: Path to the date folder

    Returns:
        Dictionary with keys for each file type, containing list of (file_path, file_name) tuples
    """
    summary_files = {
        'subject': [],
        'site': [],
        'depot': [],
        'slevel_supplymethod': [],
        'clevel_supplymethod': []
    }

    try:
        subfolders = [item for item in dbutils.fs.ls(date_folder_path) if item.isDir()]

        for subfolder in subfolders:
            subfolder_path = subfolder.path
            logger.info(f"Scanning study folder: {subfolder_path}")

            latest_in_study = {key: None for key in summary_files.keys()}

            try:
                files = dbutils.fs.ls(subfolder_path)
                for file in files:
                    file_name = file.name
                    file_name_lower = file_name.lower()

                    if not file_name_lower.endswith('.csv'):
                        continue

                    modified_time = file.modificationTime / 1000
                    modified_datetime = datetime.fromtimestamp(modified_time)

                    # Determine category
                    category = None
                    if 'subject summary' in file_name_lower:
                        category = 'subject'
                    elif 'site' in file_name_lower and 'inventory' in file_name_lower:
                        category = 'site'
                    elif 'depot' in file_name_lower and 'inventory' in file_name_lower:
                        category = 'depot'
                    elif 'site' in file_name_lower and 'supplymethod' in file_name_lower:
                        category = 'slevel_supplymethod'
                    elif 'country' in file_name_lower and 'supplymethod' in file_name_lower:
                        category = 'clevel_supplymethod'

                    if category:
                        current_entry = latest_in_study[category]
                        if current_entry is None or modified_datetime > current_entry['datetime']:
                            latest_in_study[category] = {
                                'path': file.path,
                                'name': file_name,
                                'datetime': modified_datetime
                            }

                # Add the latest file from this subfolder to overall list
                for category, info in latest_in_study.items():
                    if info:
                        summary_files[category].append((info['path'], info['name']))
                        logger.info(f"Latest {category} → {info['name']}")

            except Exception as e:
                logger.error(f"Error scanning subfolder {subfolder_path}: {str(e)}")

    except Exception as e:
        logger.error(f"Error scanning date folder {date_folder_path}: {str(e)}")

    return summary_files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schema and Configuration Mapping

# COMMAND ----------

# Define all mapping configurations
MAPPING_CONFIG = {
    "subject": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Site ID": "site_id",
            "Country": "country",
            "Parent Depot": "parent_depot",
            "Investigator": "investigator",
            "Subject Number": "subject_number",
            "Year of Birth": "year_of_birth",
            "Gender": "gender",
            "TPC": "tpc",
            "Date Randomized": "date_randomized",
            "Date Treatment Discontinued": "date_treatment_discontinued",
            "Date Crossover Enrolled": "date_crossover_enrolled",
            "Date Crossover Approved": "date_crossover_approved",
            "Date Crossover Treatment Discontinued": "date_crossover_treatment_discontinued",
            "Subject Status": "subject_status",
            "Randomized Treatment": "randomized_treatment",
            "Last Study Visit Recorded": "last_study_visit_recorded",
            "Last Study Visit Date": "last_study_visit_date",
            "Last Study Visit Number": "last_study_visit_number",
            "Next Min. Study Visit Date": "next_min_study_visit_date",
            "Next Max. Study Visit Date": "next_max_study_visit_date",
            "Additional Drug Status": "additional_drug_status",
            "Last Additional Drug Visit Recorded": "last_additional_drug_visit_recorded",
            "Last Additional Drug Visit Date": "last_additional_drug_visit_date",
            "Last Additional Drug Visit Number": "last_additional_drug_visit_number",
            "Next Min. Additional Drug Visit Date": "next_min_additional_drug_visit_date",
            "Next Max. Additional Drug Visit Date": "next_max_additional_drug_visit_date"
        },
        "date_columns": [
            "date_randomized","date_treatment_discontinued", "date_crossover_enrolled", "date_crossover_approved",
            "date_crossover_treatment_discontinued", "last_study_visit_date",
            "next_min_study_visit_date", "next_max_study_visit_date",
            "last_additional_drug_visit_date", "next_min_additional_drug_visit_date",
            "next_max_additional_drug_visit_date"
        ],
        "schema_mapping": {
            "study_protocol": StringType(),
            "site_id": IntegerType(),
            "country": StringType(),
            "parent_depot": IntegerType(),
            "investigator": StringType(),
            "subject_number": LongType(),
            "year_of_birth": IntegerType(),
            "gender": StringType(),
            "tpc": StringType(),
            "date_randomized": DateType(),
            "date_treatment_discontinued": DateType(),
            "date_crossover_enrolled": DateType(),
            "date_crossover_approved": DateType(),
            "date_crossover_treatment_discontinued": DateType(),
            "subject_status": StringType(),
            "randomized_treatment": StringType(),
            "last_study_visit_recorded": StringType(),
            "last_study_visit_date": DateType(),
            "last_study_visit_number": StringType(),
            "next_min_study_visit_date": DateType(),
            "next_max_study_visit_date": DateType(),
            "additional_drug_status": StringType(),
            "last_additional_drug_visit_recorded": StringType(),
            "last_additional_drug_visit_date": DateType(),
            "last_additional_drug_visit_number": IntegerType(),
            "next_min_additional_drug_visit_date": DateType(),
            "next_max_additional_drug_visit_date": DateType(),
            "extract_date": DateType(),
            "source_file": StringType(),
            "processed_timestamp": TimestampType()
        },
        "table_name": "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_subject_summary`"
    },

    "depot": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Depot ID": "depot_id",
            "Country": "country",
            "Depot Type": "depot_type",
            "Study Drug Type": "study_drug_type",
            "Unblinded Study Drug Name": "unblinded_study_drug_name",
            "Britestock Lot Number": "britestock_lot_number",
            "Finished Lot Number": "finished_lot_number",
            "Part Number": "part_number",
            "FP Expiry Date": "fp_expiry_date",
            "Quantity Study Drug - Requested": "quantity_study_drug_requested",
            "Quantity Study Drug - Available": "quantity_study_drug_available",
            "Quantity Study Drug - Lost": "quantity_study_drug_lost",
            "Quantity Study Drug - Damaged": "quantity_study_drug_damaged",
            "Quantity Study Drug - Quarantined": "quantity_study_drug_quarantined",
            "Quantity Study Drug - Rejected": "quantity_study_drug_rejected",
            "Quantity Study Drug - Do Not Ship": "quantity_study_drug_do_not_ship",
            "Quantity Study Drug - Expired": "quantity_study_drug_expired",
            "Quantity Study Drug - Packaged (Unavailable)": "quantity_study_drug_packaged_unavailable",
            "Quantity Study Drug - Total": "quantity_study_drug_total",
            "Approved Countries": "approved_countries"
        },
        "date_columns": ["fp_expiry_date"],
        "schema_mapping": {
            "study_protocol": StringType(),
            "depot_id": IntegerType(),
            "country": StringType(),
            "depot_type": StringType(),
            "study_drug_type": StringType(),
            "unblinded_study_drug_name": StringType(),
            "britestock_lot_number": StringType(),
            "finished_lot_number": StringType(),
            "part_number": StringType(),
            "fp_expiry_date": DateType(),
            "quantity_study_drug_requested": LongType(),
            "quantity_study_drug_available": LongType(),
            "quantity_study_drug_lost": LongType(),
            "quantity_study_drug_damaged": LongType(),
            "quantity_study_drug_quarantined": LongType(),
            "quantity_study_drug_rejected": LongType(),
            "quantity_study_drug_do_not_ship": LongType(),
            "quantity_study_drug_expired": LongType(),
            "quantity_study_drug_packaged_unavailable": LongType(),
            "quantity_study_drug_total": LongType(),
            "approved_countries": StringType(),
            "extract_date": DateType(),
            "source_file": StringType(),
            "processed_timestamp": TimestampType()
        },
        "table_name": "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_depot_inventory`"
    },

    "site": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Site ID": "site_id",
            "Country": "country",
            "Investigator": "investigator",
            "Location": "location",
            "Parent Depot": "parent_depot",
            "Site Status": "site_status",
            "Study Drug Type": "study_drug_type",
            "Unblinded Study Drug Name": "unblinded_study_drug_name",
            "Britestock Lot Number": "britestock_lot_number",
            "Finished Lot Number": "finished_lot_number",
            "Part Number": "part_number",
            "FP Expiry Date": "fp_expiry_date",
            "Quantity Study Drug - Requested": "quantity_study_drug_requested",
            "Quantity Study Drug - Available": "quantity_study_drug_available",
            "Quantity Study Drug - Assigned": "quantity_study_drug_assigned",
            "Quantity Study Drug - Lost": "quantity_study_drug_lost",
            "Quantity Study Drug - Damaged": "quantity_study_drug_damaged",
            "Quantity Study Drug - Quarantined": "quantity_study_drug_quarantined",
            "Quantity Study Drug - Rejected": "quantity_study_drug_rejected",
            "Quantity Study Drug - Do Not Dispense": "quantity_study_drug_do_not_dispense",
            "Quantity Study Drug - Expired": "quantity_study_drug_expired",
            "Quantity Study Drug - Total": "quantity_study_drug_total"
        },
        "date_columns": ["fp_expiry_date"],
        "schema_mapping": {
            "study_protocol": StringType(),
            "site_id": IntegerType(),
            "country": StringType(),
            "investigator": StringType(),
            "parent_depot": IntegerType(),
            "site_status": StringType(),
            "study_drug_type": StringType(),
            "unblinded_study_drug_name": StringType(),
            "britestock_lot_number": StringType(),
            "finished_lot_number": StringType(),
            "part_number": StringType(),
            "fp_expiry_date": DateType(),
            "quantity_study_drug_requested": LongType(),
            "quantity_study_drug_available": LongType(),
            "quantity_study_drug_assigned": LongType(),
            "quantity_study_drug_lost": LongType(),
            "quantity_study_drug_damaged": LongType(),
            "quantity_study_drug_quarantined": LongType(),
            "quantity_study_drug_rejected": LongType(),
            "quantity_study_drug_do_not_dispense": LongType(),
            "quantity_study_drug_expired": LongType(),
            "quantity_study_drug_total": LongType(),
            "extract_date": DateType(),
            "source_file": StringType(),
            "processed_timestamp": TimestampType()
        },
        "table_name": "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_site_inventory`"
    },

    "slevel_supplymethod": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Country": "country",
            "Site ID": "site_id",
            "Comparator Name": "comparator_name",
            "Site Level Supply Method": "site_level_supply_method",
            "Site Status": "site_status",
        },
        "date_columns": [],
        "schema_mapping": {
            "study_protocol": StringType(),
            "country": StringType(),
            "site_id": StringType(),
            "comparator_name": StringType(),
            "site_level_supply_method": StringType(),
            "site_status": StringType(),
            "extract_date": DateType(),
            "source_file": StringType(),
            "processed_timestamp": TimestampType()
        },
        "table_name": "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_supply_method_site_level`"
    },

    "clevel_supplymethod": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Country": "country",
            "Comparator Name": "comparator_name",
            "Country Level Supply Method": "country_level_supply_method"
        },
        "date_columns": [],
        "schema_mapping": {
            "study_protocol": StringType(),
            "country": StringType(),
            "comparator_name": StringType(),
            "country_level_supply_method": StringType(),
            "extract_date": DateType(),
            "source_file": StringType(),
            "processed_timestamp": TimestampType()
        },
        "table_name": "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_supply_method_country_level`"
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark Write Functions

# COMMAND ----------

def cast_and_write_to_delta(pandas_df, table_name: str, date_folder: str, schema_mapping:  dict, use_replace_where: bool = True):
    """
    Convert pandas DataFrame to Spark, cast types, and write to Delta table.

    Args:
        pandas_df: Pandas DataFrame to write
        table_name: Target Delta table name
        date_folder: Date folder for partition overwrite
        schema_mapping: Dictionary of column names to Spark types
        use_replace_where: If True, use replaceWhere to update only matching partitions.
                          If False, overwrite the entire table. Defaults to True.
    """
    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df)

    # Cast each column to correct type
    for col_name, data_type in schema_mapping.items():
        if col_name in spark_df.columns:
            if isinstance(data_type, DateType):
                spark_df = spark_df.withColumn(col_name, F.to_date(F.col(col_name), "yyyy-MM-dd"))
            elif isinstance(data_type, TimestampType):
                spark_df = spark_df.withColumn(col_name, F.col(col_name).cast(TimestampType()))
            else:
                spark_df = spark_df.withColumn(col_name, F.col(col_name).cast(data_type))

    # Select only columns in schema
    spark_df = spark_df.select(*schema_mapping.keys())

    # Build the write operation
    writer = (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "false")
    )

    # Conditionally add replaceWhere option
    if use_replace_where:
        writer = writer.option("replaceWhere", f"extract_date = to_date('{date_folder}', 'yyyyMMdd')")
        logger.info(f"Writing {len(pandas_df)} rows to {table_name} using replaceWhere for date {date_folder}")
    else:
        logger.info(f"Writing {len(pandas_df)} rows to {table_name} with full table overwrite")

    # Execute the write
    writer.saveAsTable(table_name)

    logger.info(f"Successfully written {len(pandas_df)} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Main Processing Loop

# COMMAND ----------

for date_folder in selected_folders:
    logger.info(f"\n{'#' * 80}")
    logger.info(f"Processing date folder: {date_folder}")
    logger.info(f"{'#' * 80}")

    date_folder_path = f"{raw_data_path}/{date_folder}"

    # Find all summary files (returns file paths and names)
    summary_files = find_latest_summary_files(date_folder_path)

    # Process Subject Summary files
    if summary_files['subject']:
        logger.info(f"\n→ Processing {len(summary_files['subject'])} Subject Summary files")

        # Load CSV files into DataFrames
        subject_dataframes = load_csv_files(summary_files['subject'])

        # Process DataFrames using curator
        subject_df = curator.process_subject_summary_batch(
            dataframes=subject_dataframes,
            date_folder=date_folder,
            column_mapping=MAPPING_CONFIG["subject"]["column_mapping"],
            date_columns=MAPPING_CONFIG["subject"]["date_columns"]
        )

        if subject_df is not None:
            cast_and_write_to_delta(
                pandas_df=subject_df,
                table_name=MAPPING_CONFIG["subject"]["table_name"],
                date_folder=date_folder,
                schema_mapping=MAPPING_CONFIG["subject"]["schema_mapping"]
            )

    # Process Depot files
    if summary_files['depot']:
        logger.info(f"\n→ Processing {len(summary_files['depot'])} Depot Inventory files")

        depot_dataframes = load_csv_files(summary_files['depot'])

        depot_df = curator.process_generic_batch(
            dataframes=depot_dataframes,
            date_folder=date_folder,
            file_type="depot",
            column_mapping=MAPPING_CONFIG["depot"]["column_mapping"],
            date_columns=MAPPING_CONFIG["depot"]["date_columns"]
        )

        if depot_df is not None:
            cast_and_write_to_delta(
                pandas_df=depot_df,
                table_name=MAPPING_CONFIG["depot"]["table_name"],
                date_folder=date_folder,
                schema_mapping=MAPPING_CONFIG["depot"]["schema_mapping"]
            )

    # Process Site files
    if summary_files['site']:
        logger.info(f"\n→ Processing {len(summary_files['site'])} Site Inventory files")

        site_dataframes = load_csv_files(summary_files['site'])

        site_df = curator.process_generic_batch(
            dataframes=site_dataframes,
            date_folder=date_folder,
            file_type="site",
            column_mapping=MAPPING_CONFIG["site"]["column_mapping"],
            date_columns=MAPPING_CONFIG["site"]["date_columns"]
        )

        if site_df is not None:
            cast_and_write_to_delta(
                pandas_df=site_df,
                table_name=MAPPING_CONFIG["site"]["table_name"],
                date_folder=date_folder,
                schema_mapping=MAPPING_CONFIG["site"]["schema_mapping"]
            )

    # Process Site-level Supply Method files
    if summary_files['slevel_supplymethod']:
        logger.info(f"\n→ Processing {len(summary_files['slevel_supplymethod'])} Site-Level Supply Method files")

        slevel_dataframes = load_csv_files(summary_files['slevel_supplymethod'])

        slevel_df = curator.process_generic_batch(
            dataframes=slevel_dataframes,
            date_folder=date_folder,
            file_type="site-level supply method",
            column_mapping=MAPPING_CONFIG["slevel_supplymethod"]["column_mapping"],
            date_columns=MAPPING_CONFIG["slevel_supplymethod"]["date_columns"]
        )

        if slevel_df is not None:
            cast_and_write_to_delta(
                pandas_df=slevel_df,
                table_name=MAPPING_CONFIG["slevel_supplymethod"]["table_name"],
                date_folder=date_folder,
                schema_mapping=MAPPING_CONFIG["slevel_supplymethod"]["schema_mapping"],
                use_replace_where=False
            )

    # Process Country-level Supply Method files
    if summary_files['clevel_supplymethod']:
        logger.info(f"\n→ Processing {len(summary_files['clevel_supplymethod'])} Country-Level Supply Method files")

        clevel_dataframes = load_csv_files(summary_files['clevel_supplymethod'])

        clevel_df = curator.process_generic_batch(
            dataframes=clevel_dataframes,
            date_folder=date_folder,
            file_type="country-level supply method",
            column_mapping=MAPPING_CONFIG["clevel_supplymethod"]["column_mapping"],
            date_columns=MAPPING_CONFIG["clevel_supplymethod"]["date_columns"]
        )

        if clevel_df is not None:
            cast_and_write_to_delta(
                pandas_df=clevel_df,
                table_name=MAPPING_CONFIG["clevel_supplymethod"]["table_name"],
                date_folder=date_folder,
                schema_mapping=MAPPING_CONFIG["clevel_supplymethod"]["schema_mapping"],
                use_replace_where=False
            )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Treatment Plan Mapping

# COMMAND ----------

treatment_group_mapping_file_path = f"/dbfs{os.path.join(legacy_raw_bkt_mount_point, treatment_group_mapping_file_path)}"
tgm_df = pd.read_excel(treatment_group_mapping_file_path, sheet_name='Treatment Group Mapping', dtype='str', engine='openpyxl')

# COMMAND ----------

# 🗺️ Column rename mapping (DataFrame → table)

column_mapping = {
    "Study Protocol": "study_protocol",
    "Randomized Treatment": "randomized_treatment",
    "Subject Status": "subject_status",
    "TPC\nTreatment of Physician's Choice\nTopotecan OR Amrubicin Choice Of Drug\nIntended TPC": "tpc",
    "Study Drug Dispensed": "study_drug_dispensed",
    "Additional Study Drug Dispensed": "additional_study_drug_dispensed",
    "Additional Study Drug Prefix": "additional_study_drug_prefix",
    "Country": "country",
    "Visit Days": "visit_days",
    "Dispensing Quantity": "dispensing_quantity",
    "Dispensing Frequency (Days)": "dispensing_frequency_days",
    "Max Cycles": "max_cycles"
}

# 🧱 Expected schema for casting
schema_mapping = {
    "study_protocol": StringType(),
    "randomized_treatment": StringType(),
    "subject_status": StringType(),
    "tpc": StringType(),
    "study_drug_dispensed": StringType(),
    "additional_study_drug_dispensed": StringType(),
    "additional_study_drug_prefix": StringType(),
    "country": StringType(),
    "visit_days": StringType(),
    "dispensing_quantity": LongType(),
    "dispensing_frequency_days": LongType(),
    "max_cycles": DoubleType()
}

# 🧩 Step 1: Rename columns
tgm_df_renamed = tgm_df.rename(columns=column_mapping)

# 🧱 Step 2: Convert pandas → Spark
spark_tgm_df = spark.createDataFrame(tgm_df_renamed)

# 🧮 Step 3: Cast each column to correct type
for col_name, data_type in schema_mapping.items():
    if col_name in spark_tgm_df.columns:
        spark_tgm_df = spark_tgm_df.withColumn(col_name, F.col(col_name).cast(data_type))

# 💾 Step 4: Truncate and load (overwrite entire table)
(
    spark_tgm_df.write
    .format("delta")
    .mode("overwrite")  # full overwrite (truncate + load)
    .option("overwriteSchema", "false")
    .saveAsTable("`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_treatment_groups`")
)

print("✅ Successfully loaded clinical_treatment_group_mapping table (truncate and load).")