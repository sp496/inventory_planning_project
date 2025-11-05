# Databricks notebook source
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import re, os
from pandas import json_normalize

# COMMAND ----------

UNITY_CATALOG_TABLE = "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_demand_plan1`"

treatment_group_mapping_table = "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_treatment_group_mapping`"
subject_summary_table = "`pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_subject_summary`"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Configuration (CHANGE THESE VALUES) ---

TIMESTAMP_COL = "processed_timestamp"

# --- Step 1: Find the Latest Date (Optimized SQL) ---
# This query is highly optimized to read the minimum amount of metadata/data necessary
# to calculate the maximum date from the specified column.

max_date_query = f"""
    SELECT 
        MAX(TO_DATE({TIMESTAMP_COL})) AS max_date 
    FROM 
        {subject_summary_table}
"""

try:
    latest_date_result = spark.sql(max_date_query).collect()
    if latest_date_result and latest_date_result[0]["max_date"] is not None:
        latest_date = latest_date_result[0]["max_date"]
        latest_date_str = str(latest_date)
        print(f"✅ Latest date found: {latest_date_str}")
    else:
        print("❌ Error: Table is empty or the timestamp column contains no valid dates.")
        exit()

except Exception as e:
    print(f"❌ An error occurred during max date calculation: {e}")
    exit()

filter_query = f"""
    SELECT 
        * FROM 
        {subject_summary_table} 
    WHERE 
        TO_DATE({TIMESTAMP_COL}) = '{latest_date_str}'
"""

df_latest_data = spark.sql(filter_query)

# Convert Spark DataFrames to Pandas DataFrames for downstream processing
df_subj = df_latest_data.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Logic To read From Delta Tables

# COMMAND ----------

# Reading files from Delta tables using Spark and converting to Pandas
try:
    # Read Delta table into Spark DataFrame
    spark_df_map = spark.table(treatment_group_mapping_table)

    # Convert Spark DataFrames to Pandas DataFrames for downstream processing
    df_map = spark_df_map.toPandas()

    print(f"SUCCESS: Read {len(df_map)} rows from Mapping Delta Table.")

except Exception as e:
    print(
        f"ERROR: Failed to read Delta tables. Please check if tables {treatment_group_mapping_table} exist and is accessible.")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC Data Load using files. #To be removed after delta table use

# COMMAND ----------

# --- Subject File Column Selection and Filtering ---
df_subj = df_subj[
    ['study_protocol', 'site_id', 'country', 'parent_depot', 'investigator', 'subject_number', 'year_of_birth',
     'gender', 'tpc', 'date_randomized', 'date_crossover_enrolled', 'date_crossover_approved',
     'date_crossover_treatment_discontinued', 'subject_status', 'randomized_treatment', 'last_study_visit_recorded',
     'last_study_visit_date', 'last_study_visit_number', 'next_min_study_visit_date', 'next_max_study_visit_date',
     'additional_drug_status', 'last_additional_drug_visit_recorded', 'last_additional_drug_visit_date',
     'last_additional_drug_visit_number', 'next_min_additional_drug_visit_date', 'next_max_additional_drug_visit_date',
     'extract_date', 'source_file', 'processed_timestamp']]

df_subj['processed_timestamp'] = df_subj['processed_timestamp'].dt.strftime('%Y-%m-%d')

# --- Step 2: Clean Columns and Initial Filtering ---
df_map.columns = df_map.columns.str.strip()
df_subj.columns = df_subj.columns.str.strip()

for c in df_map.columns:
    if "TPC" in c:
        df_map.rename(columns={c: "TPC"}, inplace=True)
        break

# df_subj["Study Protocol"] = "GS-US-592-6173"
# Filter subjects by status to exclude those who have stopped treatment
df_subj = df_subj[~df_subj["subject_status"].isin(
    ["Screen Failed", "Pre-Screened Failed", "Treatment Discontinued", "Crossover Treatment Discontinued"])]
# df_subj = df_subj[df_subj['subject_number']==90011]

df_map = df_map[
    ['study_protocol', 'randomized_treatment', 'subject_status', 'tpc', 'study_drug_dispensed',
     'additional_study_drug_dispensed', 'additional_study_drug_prefix', 'country', 'visit_days', 'dispensing_quantity',
     'dispensing_frequency_days', 'max_cycles']]


# --- Step 3: Normalize Text ---
def normalize_text(s):
    if pd.isna(s):
        return str(s)
    s = str(s)
    return s.replace("’", "'").strip().lower()


for col in ["study_protocol", "randomized_treatment", "tpc", "country"]:
    if col in df_map.columns:
        df_map[col] = df_map[col].apply(normalize_text)
    if col in df_subj.columns:
        df_subj[col] = df_subj[col].apply(normalize_text)

for col in ["study_drug_dispensed", "additional_study_drug_dispensed"]:
    if col in df_map.columns:
        df_map[col] = df_map[col].apply(normalize_text)

# --- Step 4: Merge and Calculate ---
# df_map_filtered = df_map[df_map["Study Protocol"] == "gs-us-592-6173"]
join_cols = ["study_protocol", "randomized_treatment", "tpc", "subject_status"]
# Inner merge on protocol keys
df_merged = pd.merge(df_subj, df_map, on=join_cols, how="inner")


def count_visits(v):
    if v == 'nan':
        return 0
    return len(str(v).split(","))


df_merged["visit_count_per_cycle"] = df_merged["visit_days"].apply(count_visits)
df_merged["total_medicines_required_per_cycle"] = df_merged["visit_count_per_cycle"] * df_merged["dispensing_quantity"]

# --- Step 5: Identify Individual medicine_name ---
df_merged["medicine_name"] = np.where(
    df_merged["study_drug_dispensed"] != "nan",
    df_merged["study_drug_dispensed"],
    df_merged["additional_study_drug_dispensed"]
)
df_result = df_merged[df_merged["medicine_name"] != "nan"].copy()

# --- Step 6: Summarize and Keep All Unique Columns (Generating the Plan DF) ---
id_cols = ["subject_number", "medicine_name"]
sum_col = "total_medicines_required_per_cycle"
value_cols = [col for col in df_result.columns if col not in id_cols + [sum_col]]
agg_dict = {
    **{col: 'first' for col in value_cols},
    sum_col: 'sum'
}

df_plan = (
    df_result.groupby(id_cols, dropna=False)
    .agg(agg_dict)
    .reset_index()
)

# Column Cleanup: Rename Country columns for clarity
df_plan.rename(columns={'country_x': 'subject_country'}, inplace=True)

# Clean up 'last_study_visit_number' for date projection (Fixes ValueError)
df_plan['last_study_visit_number'] = pd.to_numeric(
    df_plan['last_study_visit_number'], errors='coerce'
).fillna(0).astype(int)

df_plan['dispensing_frequency_days'] = pd.to_numeric(
    df_plan['dispensing_frequency_days'], errors='coerce'
)


# COMMAND ----------

# ----------------- LOGIC FOR PARSING -----------------
def parse_last_cycle_day(recorded_str):
    """Parses the day number (N) from strings like 'TPC C20D1' or 'Cycle 46 Day 8'.
    Defaults to 1 if unparsable or NaN/NaT.
    """
    if pd.isna(recorded_str):
        return 1
    s = str(recorded_str)
    match = re.search(r'(?:D|Day\s)(\d+)', s, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return 1
    if 'cycle' in s.lower() and not match:
        return 1
    return 1


def parse_last_cycle_number(recorded_str):
    """Parses the cycle number (N) from strings like 'TPC C20D1' or 'Cycle 46 Day 8'.
    Defaults to 0 if unparsable or NaN/NaT.
    """
    if pd.isna(recorded_str):
        return 0
    s = str(recorded_str)
    match = re.search(r'(?:C|Cycle\s)(\d+)', s, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return 0
    return 0


# Apply the new parsing functions to the plan DataFrame
df_plan['parsed_last_visit_cycle'] = df_plan['last_study_visit_recorded'].apply(parse_last_cycle_number)
df_plan['parsed_last_visit_day'] = df_plan['last_study_visit_recorded'].apply(parse_last_cycle_day)

# Add column to check for "Crossover"
df_plan['Is Crossover'] = df_plan['last_study_visit_recorded'].astype(str).str.contains('Crossover', case=False)
df_plan['Is TPC'] = df_plan['last_study_visit_recorded'].astype(str).str.contains('tpc', case=False)
# ----------------- END PARSING LOGIC -----------------


# COMMAND ----------

# --- 2. Visit Date Projection (Steps 9-10) ---

print("🔹 Step 9: Generating future visit projections...")

# Set the current date for reference
TODAY = datetime.now().date()


def project_future_visits(row):
    """
    Calculates future visit dates for a single patient/drug combination.
    The logic is corrected to prioritize the next scheduled day in the current cycle
    over TODAY's date, by comparing to last_study_visit_date.
    """

    last_visit_date = row['last_study_visit_date']
    cycle_days = row['dispensing_frequency_days']
    visit_days_str = row['visit_days']

    # CRITICAL CHECK: Return empty list if core data is missing or invalid.
    if pd.isna(cycle_days) or pd.isna(last_visit_date) or not isinstance(visit_days_str, str):
        return []
    try:  # CRITICAL FIX: Ensure visit_days_str is cleaned/standardized before splitting
        visit_days = sorted([int(day.strip())
                             for day in visit_days_str.replace("nan", "").split(',')
                             if day.strip().isdigit() and day.strip()
                             ])
    except Exception as e:
        return []

    if not visit_days:
        return []

    last_day_number = row['parsed_last_visit_day']
    current_cycle_number = row['parsed_last_visit_cycle']
    is_crossover = row['Is Crossover']
    is_tpc = row['Is TPC']
    max_cycles = row['max_cycles']

    # Calculate how many full cycles are remaining to project
    # Fallback to 5 cycles if Max Cycles is invalid, or 0 if current cycle is already past max.
    if pd.isna(max_cycles) or max_cycles < 1:
        cycles_to_project = 10  # Fallback
    else:
        cycles_to_project = int(max_cycles) - current_cycle_number
        if cycles_to_project < 0:
            cycles_to_project = 0

    try:
        visit_days = sorted([int(day.strip()) for day in str(visit_days_str).split(',')])
    except:
        return []

    # Calculate the Day 1 of the last recorded cycle (current cycle)
    time_to_subtract = timedelta(days=last_day_number - 1)
    last_cycle_day_1 = last_visit_date - time_to_subtract

    # Prefix for forecast string
    if is_crossover:
        prefix = "Crossover "
    elif is_tpc:
        prefix = "TPC"
    else:
        prefix = ""

    projected_visits = []

    # ------------------- A. PROJECT REMAINING VISITS IN CURRENT CYCLE (CORRECTED) -------------------
    remaining_days = [day for day in visit_days if day > last_day_number]

    for day in remaining_days:
        visit_date = last_cycle_day_1 + timedelta(days=day - 1)

        # FIX: Check if the projected date is after the LAST RECORDED VISIT date.
        if visit_date > last_visit_date:
            recorded_forecast_str = f"{prefix}Cycle {current_cycle_number} Day {day}"

            projected_visits.append({
                'subject_number': row['subject_number'],
                'medicine_name': row['medicine_name'],
                'Projected Cycle Number': current_cycle_number,
                'Cycle Day (Visit)': day,
                'Projected Visit Date': visit_date.strftime('%Y-%m-%d'),
                'total_medicine_required_forecast': row['total_medicines_required_per_cycle'],
                'last_study_visit_recorded_forecast': recorded_forecast_str
            })

    # ------------------- B. PROJECT NEXT FULL CYCLES (5 Cycles from the next Day 1) -------------------

    # Calculate the Day 1 of the NEXT cycle (The true Forecast Start Day 1)
    cycle_duration = timedelta(days=cycle_days)
    forecast_start_day_1 = last_cycle_day_1 + cycle_duration

    # Roll forward if the calculated start is in the past
    if forecast_start_day_1 < TODAY:
        days_since_start = (TODAY - forecast_start_day_1).days
        missed_cycles = days_since_start // cycle_days + 1
        cycle_day_1 = forecast_start_day_1 + missed_cycles * cycle_duration
    else:
        cycle_day_1 = forecast_start_day_1

    # Determine the cycle number to start the future projection from
    start_cycle_number = current_cycle_number + 1

    for cycle_offset in range(cycles_to_project):
        # Calculate the Day 1 of the current future forecast cycle
        current_cycle_day_1 = cycle_day_1 + timedelta(days=cycle_offset * cycle_days)
        current_projected_cycle = start_cycle_number + cycle_offset

        # Check for Max Cycle flag
        is_max_cycle = (max_cycles is not np.nan) and (current_projected_cycle == max_cycles)

        for day in visit_days:
            # Day 1 is current_cycle_day_1. Offset by (day - 1)
            visit_date = current_cycle_day_1 + timedelta(days=day - 1)

            # Safety check (redundant but harmless due to cycles_to_project calculation)
            if max_cycles is not np.nan and current_projected_cycle > max_cycles:
                continue

            recorded_forecast_str = f"{prefix}Cycle {current_projected_cycle} Day {day}"

            projected_visits.append({
                'subject_number': row['subject_number'],
                'medicine_name': row['medicine_name'],
                'Projected Cycle Number': current_projected_cycle,
                'Cycle Day (Visit)': day,
                'Projected Visit Date': visit_date.strftime('%Y-%m-%d'),
                'total_medicine_required_forecast': row['total_medicines_required_per_cycle'],
                'last_study_visit_recorded_forecast': recorded_forecast_str
            })

    return projected_visits


# COMMAND ----------

# all_visits = [ visit_data  for index, row in df_plan.iterrows() for visit_data in project_future_visits(row)]

all_visits = []
# Use a simple list to collect subjects that caused an error
error_subjects = []

# Replace the original list comprehension with a more robust loop
for index, row in df_plan.iterrows():
    try:
        # Extend the list with the results of the function
        all_visits.extend(project_future_visits(row))
    except IndexError as e:
        # Catch the specific error and log the subject
        error_subjects.append(row['subject_number'])
        print(f"Error: IndexError for Subject {row['subject_number']} ({row['medicine_name']}): {e}")
    except Exception as e:
        # Catch any other unexpected error and log the subject
        error_subjects.append(row['subject_number'])
        print(f"Error: Unexpected Exception for Subject {row['subject_number']} ({row['medicine_name']}): {e}")

if error_subjects:
    print(f"\nCRITICAL: The following subjects caused an error and were skipped: {set(error_subjects)}")
else:
    print("\nSUCCESS: All subjects processed without uncaught exceptions.")

# Convert the result to a DataFrame
df_visits = pd.json_normalize(all_visits)

if 'subject_number' in df_visits.columns: df_visits.dropna(subset=['subject_number'], inplace=True)
df_visits['subject_number'] = df_visits['subject_number'].astype(int)

# --- Step 10: Merge, Filter, and Export ---
print("🔹 Step 10: Merging forecast dates, filtering columns, and exporting...")

merge_cols = ['subject_number', 'medicine_name']

# Define the exact original columns to keep from df_plan
original_cols_to_keep = [
    'subject_number', 'site_id', 'parent_depot', 'subject_status', 'subject_country',
    'randomized_treatment', 'tpc', 'medicine_name',
    'study_protocol', 'visit_days', 'visit_count_per_cycle', 'dispensing_quantity',
    'dispensing_frequency_days', 'date_randomized', 'subject_status_x',
    'last_study_visit_recorded', 'last_study_visit_date', 'last_study_visit_number',
    'total_medicines_required_per_cycle',
    'study_drug_dispensed', 'additional_study_drug_dispensed',
    'parsed_last_visit_cycle', 'parsed_last_visit_day', 'processed_timestamp'
]
plan_cols_to_keep = [col for col in original_cols_to_keep if col in df_plan.columns]

# Merge the projection data with the plan details
df_final = pd.merge(
    df_visits,
    df_plan[plan_cols_to_keep],
    on=merge_cols,
    how='left'
)

# print(df_final.info())
df_final.rename(columns={'Subject Status_x': 'subject_status',
                         'study_protocol': 'study_name',
                         'Projected Visit Date': 'predicted_next_visit_date',
                         'Projected Cycle Number': 'cycle',
                         'last_study_visit_recorded_forecast': 'predicted_study_visit',
                         'medicine_name': 'drug_dispensed',
                         'Cycle Day (Visit)': 'day'
                         }, inplace=True)

# 'subject_number': row['subject_number'],
#                 'medicine_name': row['medicine_name'],
#                 'Projected Cycle Number': current_cycle_number,
#                 'Cycle Day (Visit)': day,
#                 'Projected Visit Date': visit_date.strftime('%Y-%m-%d'),
#                 'total_medicine_required_forecast': row['total_medicines_required_per_cycle'],
#                 'last_study_visit_recorded Forecast': recorded_forecast_str

# Define the final output column order

final_output_cols = ['study_name', 'parent_depot', 'site_id', 'subject_number', 'subject_status', 'subject_country',
                     'randomized_treatment', 'tpc', 'drug_dispensed', 'dispensing_quantity',
                     'predicted_study_visit', 'cycle', 'day', 'predicted_next_visit_date', 'processed_timestamp'
                     ]
final_cols = [col for col in final_output_cols if col in df_final.columns]
df_final = df_final[final_cols]
df_final = df_final.sort_values(by=['study_name', 'parent_depot', 'site_id', 'subject_number', 'cycle', 'day'])

df_final.columns = df_final.columns.str.lower().str.replace(' ', '_')

print("🎯 FINAL SCRIPT FOR Patient_Visit_Projection_Detailed_Corrected.csv")

# COMMAND ----------

display(df_final)
print(df_final.info())

# COMMAND ----------

# --- 1. Handle Date Conversion ---
# The 'predicted_next_visit_date' column must be converted to datetime objects,
# and then optionally to Python's native date objects for best Spark compatibility.

# Convert object/string column to pandas datetime, handling potential errors
df_final['predicted_next_visit_date'] = pd.to_datetime(
    df_final['predicted_next_visit_date'],
    errors='coerce'  # Set invalid parsing to NaT (Not a Time)
)

# Convert NaT values to None, as Spark handles None/nulls better than NaT
# and then extract the date part
df_final['predicted_next_visit_date'] = (
    df_final['predicted_next_visit_date']
    .apply(lambda x: x.date() if pd.notna(x) else None)
)

# --- 2. Handle Integer Conversions (INT NOT NULL) ---
# Since your SQL table specifies INT NOT NULL, the Pandas columns must be clean.
# We'll use the nullable pandas 'Int64' type first, then convert if necessary.

int_cols = ['site_id', 'parent_depot', 'subject_number', 'cycle', 'day', 'dispensing_quantity']

for col in int_cols:
    # Coerce errors to NaN (which is treated as Null in Spark) and use Int64 (nullable integer)
    df_final[col] = pd.to_numeric(
        df_final[col],
        errors='coerce'
    ).astype('Int64')

    # Since the target SQL column is NOT NULL, you must handle any remaining NaNs
    # (i.e., data quality issues where a non-numeric value was present)
    if df_final[col].isnull().any():
        print(
            f"Warning: Column '{col}' contains non-integer or missing data. This will violate the NOT NULL constraint upon writing to Unity Catalog.")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

unity_catalog_schema = StructType([
    StructField("study_name", StringType(), False),
    StructField("parent_depot", IntegerType(), False),
    StructField("site_id", IntegerType(), False),
    StructField("subject_number", IntegerType(), False),
    StructField("subject_status", StringType(), False),
    StructField("subject_country", StringType(), False),
    StructField("randomized_treatment", StringType(), False),
    StructField("tpc", StringType(), False),
    StructField("drug_dispensed", StringTypepe(), False),
    StructField("dispensing_quantity", IntegerType(), False),
    StructField("predicted_study_visit", StringType(), False),
    StructField("cycle", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("predicted_next_visit_date", DateType(), False),
    StructField("processed_timestamp", DateType(), False)

])

spark_df = spark.createDataFrame(
    df_final.rename(columns=lambda x: x.lower().replace(' ', '_')).assign(
        day=lambda df: df['day'].apply(lambda d: d.day if hasattr(d, 'day') else d),
        processed_timestamp=lambda df: pd.to_datetime(df['processed_timestamp']),
        record_created_dt=lambda df: pd.Timestamp.now()),  # Convert 'day' to int if it's a date
    schema=unity_catalog_schema  # Use the PySpark schema you defined earlier
)

print(f"\nWriting records to Unity Catalog table: {UNITY_CATALOG_TABLE}...")
spark_df.printSchema()
spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(UNITY_CATALOG_TABLE)

print("Data successfully loaded and unified.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_demand_plan`

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from `pdm-pdm-gsc-bi-dev`.`clinical_inventory`.`clinical_demand_plan`

# COMMAND ----------

