# Databricks notebook source
import os
import json
import re
from datetime import datetime
import pandas as pd
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType, IntegerType, LongType, DateType, TimestampType, DoubleType

# COMMAND ----------

env = os.environ.get('DATAENV')
print("Environment:", env)

# COMMAND ----------

with open("config.json") as f:
    config = json.load(f)


resolved_env = "prod" if env == "prd" else env

legacy_raw_bkt = config["legacy_raw_bkt"].format(env=env)
legacy_raw_bkt_mount_point = config["legacy_raw_bkt_mount_point"].rstrip("/")
ss_header_mapping_file_path = config["ss_header_mapping_file_path"].format(env=resolved_env)
treatment_group_mapping_file_path = config["treatment_group_mapping_file_path"].format(env=resolved_env)
site_level_supplymethod_file_path = config["site_level_supplymethod_file_path"].format(env=resolved_env)
country_level_supplymethod_file_path = config["country_level_supplymethod_file_path"].format(env=resolved_env)

data_bkt = config["data_bkt"].format(env=env)
data_bkt_mount_point = config["data_bkt_mount_point"].rstrip("/")

raw_data_dir = config["raw_data_dir"]

# New historical load controls
historical_load = config.get("historical_load", False)
start_date = config.get("start_date")
end_date = config.get("end_date")

# COMMAND ----------

# Ensure mounts
if not any(m.mountPoint == legacy_raw_bkt_mount_point for m in dbutils.fs.mounts()):
    dbutils.fs.mount(source=f"s3a://{legacy_raw_bkt}", mount_point=legacy_raw_bkt_mount_point)

if not any(m.mountPoint == data_bkt_mount_point for m in dbutils.fs.mounts()):
    dbutils.fs.mount(source=f"s3a://{data_bkt}", mount_point=data_bkt_mount_point)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read mapping files

# COMMAND ----------

ss_header_mapping_file_path = f"/dbfs{os.path.join(legacy_raw_bkt_mount_point, ss_header_mapping_file_path)}"

ss_header_mapping_df = pd.read_excel(ss_header_mapping_file_path, dtype='str', engine='openpyxl')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process raw data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Identify date folders to be processed

# COMMAND ----------

def get_available_date_folders(base_path):
    """Return sorted list of (folder_name, datetime_obj) tuples."""
    folders = []
    for f in dbutils.fs.ls(base_path):
        name = f.name.strip("/")
        if not name.isdigit():
            continue
        try:
            date_obj = datetime.strptime(name, "%Y%m%d")
            folders.append((name, date_obj))
        except ValueError:
            continue
    return sorted(folders, key=lambda x: x[1])

# COMMAND ----------

raw_data_path = f"{data_bkt_mount_point}/{raw_data_dir}"
folders = get_available_date_folders(raw_data_path)

if not folders:
    raise Exception(f"No valid date folders found in {raw_data_path}")

# Determine folders to process
selected_folders = []

if historical_load:
    # Convert date strings to datetime if present
    start_dt = datetime.strptime(start_date, "%Y%m%d") if start_date else None
    end_dt = datetime.strptime(end_date, "%Y%m%d") if end_date else None

    if not start_dt and not end_dt:
        # Process all available folders
        selected_folders = [f for f, _ in folders]
        print(f"Historical load enabled — processing ALL {len(selected_folders)} folders.")
    elif start_dt and not end_dt:
        selected_folders = [f for f, d in folders if d >= start_dt]
        print(f"Historical load from {start_date} onwards — {len(selected_folders)} folders.")
    elif not start_dt and end_dt:
        selected_folders = [f for f, d in folders if d <= end_dt]
        print(f"Historical load up to {end_date} — {len(selected_folders)} folders.")
    else:
        selected_folders = [f for f, d in folders if start_dt <= d <= end_dt]
        print(f"Historical load from {start_date} to {end_date} — {len(selected_folders)} folders.")
else:
    latest_folder = folders[-1][0]
    selected_folders = [latest_folder]
    print(f"Standard mode — processing latest folder only: {latest_folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process date folders

# COMMAND ----------

def find_summary_files_old(date_folder_path):
    """
    Find CSV files containing 'Subject Summary', 'Site Inventory Summary', 
    or 'Depot Inventory Summary' in their name within a date folder.
    
    Args:
        date_folder_path: Path to the date folder
        
    Returns:
        dict: Dictionary with keys 'subject', 'site', and 'depot', 
              each containing a list of tuples (file_path, file_name)
    """
    summary_files = {
        'subject': [],
        'site': [],
        'depot': []
    }
    
    try:
        # List all items in the date folder
        items = dbutils.fs.ls(date_folder_path)
        
        for item in items:
            # Check if it's a directory (subfolder)
            if item.isDir():
                subfolder_path = item.path
                print(f"  Scanning subfolder: {subfolder_path}")
                
                # List files in subfolder
                try:
                    files = dbutils.fs.ls(subfolder_path)
                    for file in files:
                        file_name = file.name
                        file_name_lower = file_name.lower()
                        
                        # Check if it's a CSV file and categorize by type
                        if file_name_lower.endswith('.csv'):
                            if 'subject summary' in file_name_lower:
                                summary_files['subject'].append((file.path, file_name))
                                print(f"    Found Subject Summary: {file_name}")
                            elif 'site' in file_name_lower and 'inventory' in file_name_lower:
                                summary_files['site'].append((file.path, file_name))
                                print(f"    Found Site Inventory: {file_name}")
                            elif 'depot' in file_name_lower and 'inventory' in file_name_lower:
                                summary_files['depot'].append((file.path, file_name))
                                print(f"    Found Depot Inventory: {file_name}")
                                
                except Exception as e:
                    print(f"    Error scanning subfolder {subfolder_path}: {str(e)}")
            else:
                # Check files directly in date folder (if any)
                file_name = item.name
                file_name_lower = file_name.lower()
                
                if file_name_lower.endswith('.csv'):
                    if 'subject summary' in file_name_lower:
                        summary_files['subject'].append((item.path, file_name))
                        print(f"  Found Subject Summary (root level): {file_name}")
                    elif 'site' in file_name_lower and 'inventory' in file_name_lower:
                        summary_files['site'].append((item.path, file_name))
                        print(f"  Found Site Inventory (root level): {file_name}")
                    elif 'depot' in file_name_lower and 'inventory' in file_name_lower:
                        summary_files['depot'].append((item.path, file_name))
                        print(f"  Found Depot Inventory (root level): {file_name}")
                    
    except Exception as e:
        print(f"Error scanning date folder {date_folder_path}: {str(e)}")
    
    return summary_files


# COMMAND ----------

def find_latest_summary_files(date_folder_path):
    """
    For each study subfolder within a date folder, find the latest CSV file for each category:
    'Subject Summary', 'Site Inventory Summary', and 'Depot Inventory Summary'.

    Args:
        date_folder_path (str): Path to the date folder

    Returns:
        dict: Dictionary with keys 'subject', 'site', and 'depot',
              each containing a list of tuples (file_path, file_name)
              representing the latest file per study folder.
    """
    summary_files = {
        'subject': [],
        'site': [],
        'depot': [],
        'slevel_supplymethod': [],
        'clevel_supplymethod': []
    }

    try:
        # Get all study subfolders
        subfolders = [item for item in dbutils.fs.ls(date_folder_path) if item.isDir()]

        for subfolder in subfolders:
            subfolder_path = subfolder.path
            print(f"\n📁 Scanning study folder: {subfolder_path}")

            latest_in_study = {
                'subject': None,
                'site': None,
                'depot': None,
                'slevel_supplymethod': None,
                'clevel_supplymethod': None
            }

            try:
                files = dbutils.fs.ls(subfolder_path)
                for file in files:
                    file_name = file.name
                    file_name_lower = file_name.lower()

                    if file_name_lower.endswith('.csv'):
                        modified_time = file.modificationTime / 1000
                        modified_datetime = datetime.fromtimestamp(modified_time)

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
                            if (
                                current_entry is None or 
                                modified_datetime > current_entry['datetime']
                            ):
                                latest_in_study[category] = {
                                    'path': file.path,
                                    'name': file_name,
                                    'datetime': modified_datetime
                                }

                # Add the latest file from this subfolder to overall list
                for category, info in latest_in_study.items():
                    if info:
                        summary_files[category].append((info['path'], info['name']))
                        print(f"   ✅ Latest {category.title()} → {info['name']}")

            except Exception as e:
                print(f"   ⚠️ Error scanning subfolder {subfolder_path}: {str(e)}")

    except Exception as e:
        print(f"❌ Error scanning date folder {date_folder_path}: {str(e)}")

    return summary_files

# find_latest_summary_files('/mnt/pdm-gsc-bi/clinical_inventory/raw/20251106')


def find_latest_summary_files_by_filename_timestamp(date_folder_path):
    """
    For each study subfolder within a date folder, find the latest CSV file for each category
    by extracting the timestamp from the filename itself (not file modification time).

    Filename format expected: *YYYY-MM-DD-HH-MM-SS.csv
    Example: Gilead GS-US-577-6153_Subject Summary (Unblinded)Subject Summary2025-11-10-08-19-19.csv

    Args:
        date_folder_path (str): Path to the date folder

    Returns:
        dict: Dictionary with keys 'subject', 'site', and 'depot',
              each containing a list of tuples (file_path, file_name)
              representing the latest file per study folder.
    """
    summary_files = {
        'subject': [],
        'site': [],
        'depot': [],
        'slevel_supplymethod': [],
        'clevel_supplymethod': []
    }

    # Regex pattern to extract timestamp from filename: YYYY-MM-DD-HH-MM-SS before .csv
    timestamp_pattern = r'(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})\.csv$'

    try:
        # Get all study subfolders
        subfolders = [item for item in dbutils.fs.ls(date_folder_path) if item.isDir()]

        for subfolder in subfolders:
            subfolder_path = subfolder.path
            print(f"\n📁 Scanning study folder: {subfolder_path}")

            latest_in_study = {
                'subject': None,
                'site': None,
                'depot': None,
                'slevel_supplymethod': None,
                'clevel_supplymethod': None
            }

            try:
                files = dbutils.fs.ls(subfolder_path)
                for file in files:
                    file_name = file.name
                    file_name_lower = file_name.lower()

                    if not file_name_lower.endswith('.csv'):
                        continue

                    # Extract timestamp from filename
                    match = re.search(timestamp_pattern, file_name)
                    if not match:
                        print(f"   ⚠️ Could not extract timestamp from filename: {file_name}")
                        continue

                    timestamp_str = match.group(1)
                    try:
                        # Parse timestamp: YYYY-MM-DD-HH-MM-SS
                        file_datetime = datetime.strptime(timestamp_str, '%Y-%m-%d-%H-%M-%S')
                    except ValueError as e:
                        print(f"   ⚠️ Could not parse timestamp '{timestamp_str}' from file {file_name}: {e}")
                        continue

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
                        if (
                            current_entry is None or
                            file_datetime > current_entry['datetime']
                        ):
                            latest_in_study[category] = {
                                'path': file.path,
                                'name': file_name,
                                'datetime': file_datetime
                            }

                # Add the latest file from this subfolder to overall list
                for category, info in latest_in_study.items():
                    if info:
                        summary_files[category].append((info['path'], info['name']))
                        print(f"   ✅ Latest {category.title()} → {info['name']} (timestamp: {info['datetime']})")

            except Exception as e:
                print(f"   ⚠️ Error scanning subfolder {subfolder_path}: {str(e)}")

    except Exception as e:
        print(f"❌ Error scanning date folder {date_folder_path}: {str(e)}")

    return summary_files


# COMMAND ----------

def read_dynamic_csv(filepath):
    # Find the first line that has all non-empty cells
    with open(filepath, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            cells = [c.strip() for c in line.split(',')]
            # Check if all cells are non-empty
            if all(cells) and len(cells) > 1:
                header_line_index = i
                break
        else:
            raise ValueError("No fully populated header line found.")

    # Read CSV from that header line
    df = pd.read_csv(filepath, dtype=str, encoding='utf-8', skiprows=header_line_index)
    return df

# COMMAND ----------

def load_mapping_from_excel(excel_path, sheet_name):
    """
    Load column mapping from an Excel file.

    Args:
        excel_path: Path to the Excel file containing mappings
        sheet_name: Name of the sheet to read (e.g., 'Header')

    Returns:
        pandas DataFrame with mapping data where:
        - First column 'Column Header' contains standardized column names
        - Each subsequent column is a Study Protocol with original column names
    """
    print(f"Loading mapping from: {excel_path}")
    print(f"Sheet: {sheet_name}")

    mapping_df = pd.read_excel(excel_path, sheet_name=sheet_name)

    # Verify 'Column Header' column exists
    if 'Column Header' not in mapping_df.columns:
        raise ValueError(f"'Column Header' column not found in mapping file. Found columns: {list(mapping_df.columns)}")

    print(f"Mapping loaded: {len(mapping_df)} rows")
    print(f"Available Study Protocols: {list(mapping_df.columns[1:])}")
    return mapping_df


def standardize_dataframe(source_df, mapping_df, filename):
    """
    Standardize a dataframe based on header mapping

    Args:
        source_df: pandas DataFrame with source data (should already have headers skipped if needed)
        mapping_df: pandas DataFrame with column mappings where:
                    - First column 'Column Header' contains standardized column names
                    - Each subsequent column is a Study Protocol (e.g., 'GS-US-592-6173')
                    - Values in protocol columns are the original column names
        filename: Original filename to extract Study Protocol from (e.g., "Gilead GS-US-592-6173_Subject Summary...")

    Returns:
        tuple: (df_standardized, study_protocol)
               - df_standardized: pandas DataFrame with standardized columns (includes Study Protocol column)
               - study_protocol: str, the study protocol value extracted from filename
    """

    # Create a copy to avoid modifying the original
    df = source_df.copy()

    # Extract Study Protocol from filename
    # Pattern: Extract text between "Gilead " and "_"
    # Example: "Gilead GS-US-592-6173_Subject Summary..." -> "GS-US-592-6173"
    match = re.search(r'GS-US-\d+-\d+', filename)
    if match:
        study_protocol = match.group(0)
        print(f"Study Protocol extracted from filename: {study_protocol}")
    else:
        raise ValueError(f"Could not extract Study Protocol from filename: {filename}")

    # Add Study Protocol column to the dataframe
    df['Study Protocol'] = study_protocol
    print(f"Added 'Study Protocol' column to dataframe")

    # Check if this study protocol exists in the mapping file
    if study_protocol not in mapping_df.columns:
        raise ValueError(
            f"Study Protocol '{study_protocol}' not found in mapping file columns. Available protocols: {list(mapping_df.columns[1:])}")

    print(f"\nFound mapping column for Study Protocol: {study_protocol}")

    # Get the standardized column names (first column)
    standardized_columns = mapping_df['Column Header'].tolist()

    # Create mapping dictionary: {original_column_name: standardized_column_name}
    # Only include non-null mappings
    column_mapping = {}
    reverse_mapping = {}  # {standardized: original} for easier lookup

    for idx, row in mapping_df.iterrows():
        std_col = row['Column Header']
        orig_col = row[study_protocol]

        # Skip if the original column is blank/null
        if pd.notna(orig_col) and str(orig_col).strip():
            column_mapping[str(orig_col).strip()] = std_col
            reverse_mapping[std_col] = str(orig_col).strip()

    print(f"\nColumn mapping loaded: {len(column_mapping)} columns")
    print("Column Mapping (showing all mappings):")
    for orig, std in column_mapping.items():
        if orig != std:
            print(f"  '{orig}' -> '{std}'")
        else:
            print(f"  '{orig}' (no change)")

    print(f"\nOriginal shape: {df.shape}")
    print(f"Original columns: {list(df.columns)}")

    # Rename columns based on mapping
    print("\nStandardizing column names...")
    columns_to_rename = {}
    for col in df.columns:
        if col == 'Study Protocol':
            continue  # Skip the Study Protocol column we just added
        col_stripped = col.strip()
        if col_stripped in column_mapping:
            standardized_name = column_mapping[col_stripped]
            if col != standardized_name:
                columns_to_rename[col] = standardized_name
                print(f"  Renaming: '{col}' -> '{standardized_name}'")

    if columns_to_rename:
        df = df.rename(columns=columns_to_rename)
        print(f"  Renamed {len(columns_to_rename)} columns")
    else:
        print("  No columns needed renaming (already standardized)")

    # Create standardized dataframe with all standard columns
    # Include 'Study Protocol' as the first column
    final_columns = ['Study Protocol'] + standardized_columns

    print(f"\nCreating standardized dataframe with {len(final_columns)} columns...")
    df_standardized = pd.DataFrame(index=df.index)

    # Add Study Protocol column first
    df_standardized['Study Protocol'] = study_protocol

    # Add each standardized column
    for std_col in standardized_columns:
        if std_col in df.columns:
            # Column exists in the data
            df_standardized[std_col] = df[std_col]
            print(f"  ✓ '{std_col}' - populated from data")
        else:
            # Column doesn't exist - create empty column
            df_standardized[std_col] = None
            print(f"  ⚠ '{std_col}' - not found, created as blank")

    print(f"\nFinal shape: {df_standardized.shape}")
    print(f"Final columns: {list(df_standardized.columns)}")

    return df_standardized, study_protocol

# COMMAND ----------

def remove_rows_with_n_values(df, n=1):
    non_null_counts = df.notna().sum(axis=1)
    df = df[non_null_counts > n]
    return df.reset_index(drop=True)

def process_subject_summary_file(file_path, file_name, mapping_df):
    """
    Read a Subject Summary CSV file and apply standardization.
    
    Args:
        file_path: Full path to the CSV file
        file_name: Name of the file
        mapping_df: Mapping dataframe for standardization
        
    Returns:
        tuple: (standardized_df, study_protocol) or (None, None) if processing fails
    """
    try:
        print(f"\n{'='*80}")
        print(f"Processing file: {file_name}")
        print(f"Path: {file_path}")
        print(f"{'='*80}")
        
        # Convert dbfs path to local path for pandas
        local_path = file_path.replace("dbfs:", "/dbfs")
        
        # Read CSV file into pandas dataframe
        # df = pd.read_csv(local_path, dtype=str, encoding='utf-8', skiprows=1)
        df = read_dynamic_csv(local_path)

        print(f"Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")
        
        # Apply standardization function
        standardized_df, study_protocol = standardize_dataframe(df, mapping_df, file_name)
        
        print(f"✓ Successfully standardized file")
        print(f"  Study Protocol: {study_protocol}")
        print(f"  Output shape: {standardized_df.shape}")
        
        return standardized_df, study_protocol
        
    except Exception as e:
        print(f"✗ Error processing file {file_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None
    
    
def process_depot_file(file_path, file_name):

    try:
        print(f"\n{'='*80}")
        print(f"Processing file: {file_name}")
        print(f"Path: {file_path}")
        print(f"{'='*80}")
        
        # Convert dbfs path to local path for pandas
        local_path = file_path.replace("dbfs:", "/dbfs")
        
        # Read CSV file into pandas dataframe
        # df = pd.read_csv(local_path, dtype=str, encoding='utf-8', skiprows=1)
        df = read_dynamic_csv(local_path)

        print(df.head())
        print(f"Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")

        match = re.search(r'GS-US-\d+-\d+', file_name)
        if match:
            study_protocol = match.group(0)
            print(f"Study Protocol extracted from filename: {study_protocol}")
        else:
            raise ValueError(f"Could not extract Study Protocol from filename: {file_name}")

        # Add Study Protocol column to the dataframe
        df.insert(0, 'Study Protocol', study_protocol)
        
        print(f"✓ Successfully standardized file")
        print(f"  Study Protocol: {study_protocol}")
        print(f"  Output shape: {df.shape}")
        # print(standardized_df.head().to_string())
        return df, study_protocol
        
    except Exception as e:
        print(f"✗ Error processing file {file_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None
    

def process_site_file(file_path, file_name):

    try:
        print(f"\n{'='*80}")
        print(f"Processing file: {file_name}")
        print(f"Path: {file_path}")
        print(f"{'='*80}")
        
        # Convert dbfs path to local path for pandas
        local_path = file_path.replace("dbfs:", "/dbfs")
        
        # Read CSV file into pandas dataframe
        # df = pd.read_csv(local_path, dtype=str, encoding='utf-8', skiprows=1)
        df = read_dynamic_csv(local_path)
        # df = remove_rows_with_n_values(df)
        print(df.head())
        print(f"Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")

        match = re.search(r'GS-US-\d+-\d+', file_name)
        if match:
            study_protocol = match.group(0)
            print(f"Study Protocol extracted from filename: {study_protocol}")
        else:
            raise ValueError(f"Could not extract Study Protocol from filename: {file_name}")

        # Add Study Protocol column to the dataframe
        df.insert(0, 'Study Protocol', study_protocol)
        
        print(f"✓ Successfully standardized file")
        print(f"  Study Protocol: {study_protocol}")
        print(f"  Output shape: {df.shape}")
        # print(standardized_df.head().to_string())
        return df, study_protocol
        
    except Exception as e:
        print(f"✗ Error processing file {file_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None
    

def process_supplymethod_file(file_path, file_name):

    try:
        print(f"\n{'='*80}")
        print(f"Processing file: {file_name}")
        print(f"Path: {file_path}")
        print(f"{'='*80}")
        
        # Convert dbfs path to local path for pandas
        local_path = file_path.replace("dbfs:", "/dbfs")
        
        # Read CSV file into pandas dataframe
        df = read_dynamic_csv(local_path)
        print(df.head())
        print(f"Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")

        match = re.search(r'GS-US-\d+-\d+', file_name)
        if match:
            study_protocol = match.group(0)
            print(f"Study Protocol extracted from filename: {study_protocol}")
        else:
            raise ValueError(f"Could not extract Study Protocol from filename: {file_name}")

        # Add Study Protocol column to the dataframe
        df.insert(0, 'Study Protocol', study_protocol)
        
        print(f"✓ Successfully standardized file")
        print(f"  Study Protocol: {study_protocol}")
        print(f"  Output shape: {df.shape}")
        # print(standardized_df.head().to_string())
        return df, study_protocol
        
    except Exception as e:
        print(f"✗ Error processing file {file_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None

# COMMAND ----------

mapping_config = {
    "subject": 
        {
        "column_mapping": 
            {
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
        
        "schema_mapping":
            {
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
            }
        },

    "depot":
        {
        "column_mapping": 
            {
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
        
        "schema_mapping":
            {
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
            }
        },

    "site":
        {
        "column_mapping": 
            {
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
        
        "schema_mapping":
            {
                "study_protocol": StringType(),
                "site_id": IntegerType(),
                "country": StringType(),
                "investigator": StringType(),
                # "location": StringType(),
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
            }
        },

    "slevel_supplymethod": 
        {
        "column_mapping":
            {
                "Study Protocol": "study_protocol",
                "Country": "country",
                "Site ID": "site_id",
                "Comparator Name": "comparator_name",
                "Site Level Supply Method": "site_level_supply_method",
                "Site Status": "site_status",
            },
        
        "schema_mapping":
            {
                "study_protocol": StringType(),
                "country": StringType(),
                "site_id": StringType(),
                "comparator_name": StringType(),
                "site_level_supply_method": StringType(),
                "site_status": StringType(),
                "extract_date": DateType(),
                "source_file": StringType(),
                "processed_timestamp": TimestampType()
            }
        },

    "clevel_supplymethod": 
        {
        "column_mapping":
            {
                "Study Protocol": "study_protocol",
                "Country": "country",
                "Comparator Name": "comparator_name",
                "Country Level Supply Method": "country_level_supply_method"
            },
        
        "schema_mapping":
            {
                "study_protocol": StringType(),
                "country": StringType(),
                "comparator_name": StringType(),
                "country_level_supply_method": StringType(),
                "extract_date": DateType(),
                "source_file": StringType(),
                "processed_timestamp": TimestampType()
            }
        }
}


 

# COMMAND ----------

def cast_and_write_spark_df_to_partition(spark_df, table, date_folder, schema_mapping):
    # 🧮 Cast each column to correct type
    for col_name, data_type in schema_mapping.items():
        if col_name in spark_df.columns:
            if isinstance(data_type, DateType):
                spark_df = spark_df.withColumn(col_name, F.to_date(F.col(col_name), "yyyy-MM-dd"))
            elif isinstance(data_type, TimestampType):
                spark_df = spark_df.withColumn(col_name, F.col(col_name).cast(TimestampType()))
            else:
                spark_df = spark_df.withColumn(col_name, F.col(col_name).cast(data_type))

    spark_df = spark_df.select(*schema_mapping.keys())

    # 💾 Write data to Delta table — overwrite only matching load_date
    (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "false")
        .option("replaceWhere", f"extract_date = to_date('{date_folder}', 'yyyyMMdd')")
        .saveAsTable(table)
    )

    print(f"✅ Successfully written data for load_date = {date_folder}")


# COMMAND ----------

def process_subject_summary_data(subject_files, date_folder, column_mapping, schema_mapping, table_name):

    if not subject_files:
        print(f"⚠ No Subject Summary CSV files found in {date_folder}")

    subject_summary_data = []

    for file_path, file_name in subject_files:
        subject_standardized_df, study_protocol = process_subject_summary_file(
            file_path, file_name, ss_header_mapping_df
        )

        if subject_standardized_df is not None:
            subject_standardized_df['source_file'] = file_name
            subject_summary_data.append(subject_standardized_df)
    
    if subject_summary_data:
        # 🧩 Combine all files for the date_folder
        subject_summary_data = pd.concat(subject_summary_data, ignore_index=True)

        subject_summary_data['extract_date'] = pd.to_datetime(date_folder, format='%Y%m%d').strftime('%Y-%m-%d')
        subject_summary_data['processed_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 🧭 Rename columns to match Delta table
        subject_summary_data = subject_summary_data.rename(columns=column_mapping)

        date_columns = [
                    "date_randomized",
                    "date_crossover_enrolled",
                    "date_crossover_approved",
                    "date_crossover_treatment_discontinued",
                    "last_study_visit_date",
                    "next_min_study_visit_date",
                    "next_max_study_visit_date",
                    "last_additional_drug_visit_date",
                    "next_min_additional_drug_visit_date",
                    "next_max_additional_drug_visit_date"
                ]

        # Convert dates
        subject_summary_data[date_columns] = subject_summary_data[date_columns].apply(
            lambda col: pd.to_datetime(col, format='%d-%b-%Y', errors='coerce'))

        # 🧱 Convert to Spark DataFrame
        spark_df = spark.createDataFrame(subject_summary_data)
 
        cast_and_write_spark_df_to_partition(spark_df, table_name, date_folder, schema_mapping)


def process_depot_data(depot_files, date_folder, column_mapping, schema_mapping, table_name):

    if not depot_files:
        print(f"⚠ No Depot inventory Summary CSV files found in {date_folder}")

    depot_data = []

    for file_path, file_name in depot_files:
        depot_standardized_df, study_protocol = process_depot_file(file_path, file_name)
        if depot_standardized_df is not None:
            depot_standardized_df['source_file'] = file_name
            depot_data.append(depot_standardized_df)

    if depot_data:
        # 🧩 Combine all files for the date_folder
        depot_data = pd.concat(depot_data, ignore_index=True)
        depot_data['extract_date'] = pd.to_datetime(date_folder, format='%Y%m%d').strftime('%Y-%m-%d')
        depot_data['processed_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 🧭 Rename columns to match Delta table
        depot_data = depot_data.rename(columns=column_mapping)

        date_columns = [
            "fp_expiry_date"
        ]

        # Convert dates
        depot_data[date_columns] = depot_data[date_columns].apply(
            lambda col: pd.to_datetime(col, format='%d-%b-%Y', errors='coerce'))

        # 🧱 Convert to Spark DataFrame
        spark_df = spark.createDataFrame(depot_data)
 
        cast_and_write_spark_df_to_partition(spark_df, table_name, date_folder, schema_mapping)
        

def process_site_data(site_files, date_folder, column_mapping, schema_mapping, table_name):

    if not site_files:
        print(f"⚠ No site inventory Summary CSV files found in {date_folder}")

    site_data = []

    for file_path, file_name in site_files:
        site_standardized_df, study_protocol = process_site_file(file_path, file_name)
        if site_standardized_df is not None:
            site_standardized_df['source_file'] = file_name
            site_data.append(site_standardized_df)

    if site_data:
        # 🧩 Combine all files for the date_folder
        site_data = pd.concat(site_data, ignore_index=True)
        site_data['extract_date'] = pd.to_datetime(date_folder, format='%Y%m%d').strftime('%Y-%m-%d')
        site_data['processed_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 🧭 Rename columns to match Delta table
        site_data.rename(columns=column_mapping, inplace=True)

        date_columns = [
            "fp_expiry_date"
            ]

        # Convert dates
        site_data[date_columns] = site_data[date_columns].apply(
            lambda col: pd.to_datetime(col, format='%d-%b-%Y', errors='coerce'))

        # 🧱 Convert to Spark DataFrame
        spark_df = spark.createDataFrame(site_data)

 
        cast_and_write_spark_df_to_partition(spark_df, table_name, date_folder, schema_mapping)
        

def process_supplymethod_data(supplymethod_files, date_folder, column_mapping, schema_mapping, table_name):

    if not supplymethod_files:
        print(f"⚠ No Supply method CSV files found in {date_folder}")

    supplymethod_data = []

    for file_path, file_name in supplymethod_files:
        supplymethod_standardized_df, study_protocol = process_site_file(file_path, file_name)
        if supplymethod_standardized_df is not None:
            supplymethod_standardized_df['source_file'] = file_name
            supplymethod_data.append(supplymethod_standardized_df)

    if supplymethod_data:
        # 🧩 Combine all files for the date_folder
        supplymethod_data = pd.concat(supplymethod_data, ignore_index=True)
        supplymethod_data['extract_date'] = pd.to_datetime(date_folder, format='%Y%m%d').strftime('%Y-%m-%d')
        supplymethod_data['processed_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 🧭 Rename columns to match Delta table
        supplymethod_data.rename(columns=column_mapping, inplace=True)

        # date_columns = [
        #     "fp_expiry_date"
        #     ]

        # # Convert dates
        # supplymethod_data[date_columns] = supplymethod_data[date_columns].apply(
        #     lambda col: pd.to_datetime(col, format='%d-%b-%Y', errors='coerce'))

        # 🧱 Convert to Spark DataFrame
        spark_df = spark.createDataFrame(supplymethod_data)

        cast_and_write_spark_df_to_partition(spark_df, table_name, date_folder, schema_mapping)

        

# COMMAND ----------

# 🌀 3. Main loop
for date_folder in selected_folders:
    print(f"\n{'#'*80}")
    print(f"Processing date folder: {date_folder}")
    print(f"{'#'*80}")

    date_folder_path = f"{raw_data_path}/{date_folder}"
    
    summary_files = find_latest_summary_files(date_folder_path)

    process_subject_summary_data(summary_files['subject'],
                                  date_folder,
                                  mapping_config["subject"]["column_mapping"],
                                  mapping_config["subject"]["schema_mapping"],
                                  "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_subject_summary`")


    process_depot_data(summary_files['depot'],
                        date_folder,
                        mapping_config["depot"]["column_mapping"],
                        mapping_config["depot"]["schema_mapping"],
                        "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_depot_inventory`")

    
    process_site_data(summary_files['site'],
                        date_folder,
                        mapping_config["site"]["column_mapping"],
                        mapping_config["site"]["schema_mapping"],
                        "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_site_inventory`")
    

    process_supplymethod_data(summary_files['slevel_supplymethod'],
                    date_folder,
                    mapping_config["slevel_supplymethod"]["column_mapping"],
                    mapping_config["slevel_supplymethod"]["schema_mapping"],
                    "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_supply_method_site_level`"
                    )
    

    process_supplymethod_data(summary_files['clevel_supplymethod'],
                    date_folder,
                    mapping_config["clevel_supplymethod"]["column_mapping"],
                    mapping_config["clevel_supplymethod"]["schema_mapping"],
                    "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_supply_method_country_level`")


 

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