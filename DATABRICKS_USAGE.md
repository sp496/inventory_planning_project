# Using demand_planning.py in Databricks

## Quick Start

### Option 1: Simple Usage (Recommended)

```python
# Upload demand_planning.py to your Databricks workspace or DBFS

# Cell 1: Import the function
from demand_planning import run_demand_planning

# Cell 2: Load your data from Delta tables
df_subjects = spark.table("`your-catalog`.`your-schema`.`clinical_subject_summary`").toPandas()
df_mapping = spark.table("`your-catalog`.`your-schema`.`clinical_treatment_mapping`").toPandas()

# Cell 3: Run demand planning
df_forecast = run_demand_planning(df_subjects, df_mapping)

# Cell 4: Display results
display(df_forecast)

# Cell 5 (Optional): Convert back to Spark DataFrame and save
spark_df = spark.createDataFrame(df_forecast)
spark_df.write.mode("overwrite").saveAsTable("`your-catalog`.`your-schema`.`demand_forecast`")
```

### Option 2: Advanced Usage with Custom Processor

```python
from demand_planning import DemandPlanningProcessor

# Initialize processor
processor = DemandPlanningProcessor()

# Load data
df_subjects = spark.table("clinical_subject_summary").toPandas()
df_mapping = spark.table("clinical_treatment_mapping").toPandas()

# Run with more control
df_forecast = processor.run(
    df_subjects=df_subjects,
    df_mapping=df_mapping,
    output_file="/dbfs/tmp/forecast_output.csv"  # Optional: save to DBFS
)

# Display results
display(df_forecast)
```

## Data Preparation

**Important:** When you pass DataFrames to `run_demand_planning()`, the following preparation steps are automatically applied:

### For df_subjects:
1. Column names are stripped of whitespace
2. Filters to only required columns (see Config.SUBJECT_COLUMNS)
3. All date columns are parsed using `pd.to_datetime()`

### For df_mapping:
1. Column names are stripped of whitespace
2. Filters to only required columns (see Config.MAPPING_COLUMNS)
3. TPC column variations (e.g., "TPC", " TPC ", "tpc") are normalized to lowercase "tpc"

This means your Databricks DataFrames don't need to be pre-cleaned - the module handles it automatically.

## Expected Input DataFrames

### df_subjects (Subject Summary)
Required columns:
- `study_protocol`
- `site_id`
- `country`
- `parent_depot`
- `subject_number`
- `subject_status`
- `randomized_treatment`
- `tpc`
- `last_study_visit_recorded`
- `last_study_visit_date`
- `date_randomized`
- `processed_timestamp`
- ... (see Config.SUBJECT_COLUMNS for full list)

### df_mapping (Treatment Group Mapping)
Required columns:
- `study_protocol`
- `randomized_treatment`
- `subject_status`
- `tpc`
- `study_drug_dispensed`
- `additional_study_drug_dispensed`
- `visit_days` (comma-separated, e.g., "1,8,15")
- `dispensing_quantity`
- `dispensing_frequency_days`
- `max_cycles` (optional - if not defined, only time horizon applies)

## Output DataFrame

Returns a DataFrame with projected visits containing:
- `study_name`
- `parent_depot`
- `site_id`
- `subject_number`
- `subject_status`
- `subject_country`
- `randomized_treatment`
- `tpc`
- `drug_dispensed`
- `dispensing_quantity`
- `predicted_study_visit` (e.g., "Cycle 10 Day 1")
- `cycle`
- `day`
- `predicted_next_visit_date` (YYYY-MM-DD format)
- `processed_timestamp`

## Projection Logic

1. **Time-based projection**: Projects visits for next **365 days** from today
2. **Optional cycle cap**: If `max_cycles` is defined in mapping, visits beyond that cycle are filtered out
3. **Roll-forward**: Automatically catches up patients with outdated last visit dates

## Troubleshooting

### "No module named 'demand_planning'"
- Make sure `demand_planning.py` is uploaded to your workspace or DBFS
- Use `%run /path/to/demand_planning.py` to make it available

### "Either df_subjects or subject_file must be provided"
- Make sure you're passing dataframes to the function
- Check that dataframes are not None

### Timezone issues
- The system uses `datetime.now().date()` for TODAY
- Make sure your Databricks cluster timezone is set correctly if needed
