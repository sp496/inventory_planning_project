# Clinical Inventory Data Curation - Refactored

This directory contains the refactored data curation code with clear separation between file I/O and data processing, enabling local debugging and testing.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Databricks Notebook                       │
│  - Handles ALL file I/O (using dbutils)                     │
│  - Mounts S3 buckets                                         │
│  - Reads CSV files → pandas DataFrames                       │
│  - Passes DataFrames to DataCurator                          │
│  - Converts results to Spark DataFrames                      │
│  - Writes to Delta tables                                    │
└──────────────────┬──────────────────────────────────────────┘
                   │ DataFrames
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                      DataCurator Class                       │
│  - Accepts DataFrames as input (NOT file paths)             │
│  - Pure pandas processing logic                              │
│  - No file I/O, no Databricks dependencies                   │
│  - Standardization, mapping, transformations                 │
│  - Returns processed DataFrames                              │
└─────────────────────────────────────────────────────────────┘
                   ▲
                   │ DataFrames
┌──────────────────┴──────────────────────────────────────────┐
│                     Local Runner                             │
│  - Reads local CSV files → pandas DataFrames                │
│  - Passes DataFrames to DataCurator                          │
│  - Saves results to local CSV files                          │
│  - For debugging and reproducing issues                      │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Principle

**DataCurator is DataFrame-centric, not file-centric:**
- ✅ Accepts `pd.DataFrame` as input
- ✅ Returns `pd.DataFrame` as output
- ❌ Does NOT handle file reading/writing
- ❌ No dependency on Databricks (dbutils, dbfs, etc.)

This design enables:
1. **Local Debugging**: Run the same logic locally with CSV files
2. **Unit Testing**: Test with in-memory DataFrames
3. **Flexibility**: Use with any data source (files, databases, APIs)

## Structure

```
curation/
├── data_curator.py             # DataFrame processing class (pure pandas)
├── curate_data_refactored.py   # Databricks notebook (file I/O + Spark)
├── local_runner.py              # Local debugging script
├── quick_debug.py               # Simple script for quick debugging
├── test_data_curator.py         # Unit tests
├── QUICK_START.md               # Quick reference guide
└── README.md                    # This file (comprehensive guide)
```

## ⚡ Quick Start - Easiest Way to Debug Locally

**Just pass file paths directly - no manual file reading needed!**

```python
from curation.data_curator import DataCurator, load_excel_mapping

# Load mapping
mapping_df = load_excel_mapping('./test_data/header_mapping.xlsx')
curator = DataCurator(mapping_df=mapping_df)

# Process files - just pass the file paths!
result = curator.process_subject_summary_batch_from_files(
    file_paths=[
        './test_data/GS-US-592-6173_Subject_Summary.csv',
        './test_data/GS-US-592-6789_Subject_Summary.csv'
    ],
    date_folder='20251106',
    column_mapping={"Study Protocol": "study_protocol", ...},
    date_columns=["date_randomized"]
)

# Done! Inspect or save result
print(result.head())
result.to_csv('./output.csv', index=False)
```

**Or use the pre-built script:**
```bash
# 1. Edit curation/quick_debug.py to point to your files
# 2. Run it:
python curation/quick_debug.py
```

See [QUICK_START.md](QUICK_START.md) for more examples.

## Usage

### In Databricks

The notebook handles file I/O and passes DataFrames to the curator:

```python
from curation.data_curator import DataCurator, load_excel_mapping

# 1. Load mapping
mapping_df = load_excel_mapping('/dbfs/path/to/mapping.xlsx')
curator = DataCurator(mapping_df=mapping_df)

# 2. Read CSV files (Databricks handles this)
df1 = pd.read_csv('/dbfs/path/to/file1.csv')
df2 = pd.read_csv('/dbfs/path/to/file2.csv')

# 3. Pass DataFrames to curator
dataframes = [(df1, 'file1.csv'), (df2, 'file2.csv')]
result_df = curator.process_subject_summary_batch(
    dataframes=dataframes,
    date_folder='20251106',
    column_mapping=mapping_config,
    date_columns=date_cols
)

# 4. Convert to Spark and write to Delta
spark_df = spark.createDataFrame(result_df)
spark_df.write.saveAsTable('my_table')
```

### Local Debugging

When you encounter an error in Databricks, reproduce it locally:

```bash
# 1. Download the problematic CSV files from Databricks/S3 to local disk
# 2. Download the mapping Excel file

# 3. Run the local runner
python curation/local_runner.py \
    --data-dir ./test_data/20251106 \
    --date-folder 20251106 \
    --mapping ./test_data/mappings/header_mapping.xlsx \
    --output-dir ./output \
    --file-type subject

# Output will be saved to ./output/subject_summary_20251106.csv
# All logging will show you exactly where the error occurs
```

### Programmatic Local Usage

```python
from curation.data_curator import (
    DataCurator,
    read_dynamic_csv,
    load_excel_mapping
)

# Load mapping
mapping_df = load_excel_mapping('./mappings/header_mapping.xlsx')
curator = DataCurator(mapping_df=mapping_df)

# Read local CSV files
df1 = read_dynamic_csv('./data/GS-US-592-6173_Subject_Summary.csv')
df2 = read_dynamic_csv('./data/GS-US-592-6789_Subject_Summary.csv')

# Process DataFrames
dataframes = [(df1, 'GS-US-592-6173_Subject_Summary.csv'),
              (df2, 'GS-US-592-6789_Subject_Summary.csv')]

result = curator.process_subject_summary_batch(
    dataframes=dataframes,
    date_folder='20251106',
    column_mapping={
        "Study Protocol": "study_protocol",
        "Site ID": "site_id",
        # ... rest of mapping
    },
    date_columns=["date_randomized", "date_crossover_enrolled"]
)

# result is a pandas DataFrame - do whatever you want with it
result.to_csv('./output.csv', index=False)
print(result.head())
```

## Key Improvements

### 1. Clear Separation of Concerns

**Before:**
```python
# DataCurator was responsible for file I/O
def process_file(self, file_path, file_name):
    local_path = file_path.replace("dbfs:", "/dbfs")  # Databricks-specific
    df = pd.read_csv(local_path)  # File I/O
    # ...process...
```

**After:**
```python
# Databricks handles file I/O
df = read_csv_with_dynamic_header(dbfs_path)

# DataCurator just processes DataFrames
result_df = curator.process_subject_summary_batch(
    dataframes=[(df, filename)],
    date_folder=date_folder,
    column_mapping=mapping,
    date_columns=date_cols
)
```

### 2. Local Reproducibility

When you get an error in Databricks:

**Before:** You had to debug directly in Databricks (slow iteration)

**After:**
1. Download the problematic CSV file
2. Run `local_runner.py` with your CSV
3. Get the exact same error locally
4. Debug with your favorite Python debugger (PyCharm, VS Code, ipdb)
5. Fix the issue
6. Test locally
7. Deploy to Databricks

### 3. Unit Testing

**Before:** Hard to test (required Databricks environment or mocking file systems)

**After:** Easy to test with in-memory DataFrames:

```python
def test_standardize_subject_summary():
    # Create test DataFrame in memory
    df = pd.DataFrame({
        'Site': ['101', '102'],
        'Country': ['USA', 'Canada']
    })

    curator = DataCurator(mapping_df=mapping)
    result, protocol = curator.standardize_subject_summary(
        df,
        'GS-US-592-6173_Subject_Summary.csv'
    )

    assert 'Study Protocol' in result.columns
    assert protocol == 'GS-US-592-6173'
```

### 4. Code Reduction

- **DataCurator**: ~525 lines (pure processing logic)
- **Databricks Notebook**: ~800 lines (file I/O + Spark operations)
- **Local Runner**: ~320 lines (local debugging)

Original code: ~1,160 lines with duplication
Refactored code: ~1,645 lines with ZERO duplication and full local testing capability

The increase in lines gives you:
- Full local reproducibility
- Unit testing infrastructure
- Better separation of concerns
- Comprehensive logging and error handling

## Local Runner Options

```bash
# Process all file types
python curation/local_runner.py \
    --data-dir ./data/20251106 \
    --date-folder 20251106 \
    --mapping ./mappings/header_mapping.xlsx

# Process only subject summaries
python curation/local_runner.py \
    --data-dir ./data/20251106 \
    --date-folder 20251106 \
    --mapping ./mappings/header_mapping.xlsx \
    --file-type subject

# Process only depot files
python curation/local_runner.py \
    --data-dir ./data/20251106 \
    --date-folder 20251106 \
    --mapping ./mappings/header_mapping.xlsx \
    --file-type depot

# Custom output directory
python curation/local_runner.py \
    --data-dir ./data/20251106 \
    --date-folder 20251106 \
    --mapping ./mappings/header_mapping.xlsx \
    --output-dir ./my_output
```

## DataCurator API

### Core Methods

#### `standardize_subject_summary(df, filename)`
Standardizes a Subject Summary DataFrame using the mapping file.
- **Input**: DataFrame, filename (for protocol extraction)
- **Output**: (standardized_df, study_protocol)

#### `add_study_protocol_column(df, filename)`
Adds Study Protocol column (for depot, site, supply method files).
- **Input**: DataFrame, filename
- **Output**: (df_with_protocol, study_protocol)

#### `process_subject_summary_batch(dataframes, date_folder, column_mapping, date_columns)`
Process multiple Subject Summary DataFrames.
- **Input**: List of (DataFrame, filename) tuples
- **Output**: Combined processed DataFrame

#### `process_generic_batch(dataframes, date_folder, file_type, column_mapping, date_columns)`
Process multiple generic DataFrames (depot, site, supply method).
- **Input**: List of (DataFrame, filename) tuples
- **Output**: Combined processed DataFrame

### Static Utility Methods

- `extract_study_protocol(filename)` - Parse study protocol from filename
- `remove_rows_with_n_values(df, n)` - Remove sparse rows
- `convert_date_columns(df, date_columns, format)` - Convert date columns
- `add_metadata_columns(df, date_folder, source_file)` - Add metadata

### Helper Functions (for local use)

- `read_dynamic_csv(filepath)` - Read CSV with header detection
- `load_excel_mapping(excel_path, sheet_name)` - Load mapping file
- `load_treatment_mapping(excel_path, sheet_name)` - Load treatment mapping

## Debugging Workflow

### Scenario: Error in Databricks

1. **Identify the problematic file** from Databricks logs:
   ```
   Error processing Subject Summary GS-US-592-6173_Subject_Summary_20251106.csv: ...
   ```

2. **Download the file** from DBFS/S3:
   ```bash
   dbfs cp dbfs:/mnt/data/raw/20251106/.../GS-US-592-6173_Subject_Summary.csv ./test_data/
   ```

3. **Download mapping file**:
   ```bash
   dbfs cp dbfs:/mnt/config/header_mapping.xlsx ./test_data/
   ```

4. **Run locally**:
   ```bash
   python curation/local_runner.py \
       --data-dir ./test_data \
       --date-folder 20251106 \
       --mapping ./test_data/header_mapping.xlsx \
       --file-type subject
   ```

5. **You'll see the same error** with full traceback

6. **Debug interactively**:
   ```python
   # debug_script.py
   from curation.data_curator import DataCurator, read_dynamic_csv, load_excel_mapping

   # Load data
   df = read_dynamic_csv('./test_data/GS-US-592-6173_Subject_Summary.csv')
   mapping_df = load_excel_mapping('./test_data/header_mapping.xlsx')
   curator = DataCurator(mapping_df=mapping_df)

   # Step through with debugger
   import pdb; pdb.set_trace()
   result, protocol = curator.standardize_subject_summary(df, 'GS-US-592-6173_Subject_Summary.csv')
   ```

7. **Fix the issue** in `data_curator.py`

8. **Test locally** until it works

9. **Deploy to Databricks** with confidence

## Testing

Run unit tests:
```bash
python -m pytest curation/test_data_curator.py -v
```

Run integration test locally:
```bash
python curation/local_runner.py \
    --data-dir ./test_data \
    --date-folder 20251106 \
    --mapping ./test_data/header_mapping.xlsx
```

## Benefits Summary

✅ **Local Debugging**: Reproduce any Databricks error locally
✅ **Fast Iteration**: No need to deploy to Databricks to test changes
✅ **Unit Testing**: Test with in-memory DataFrames
✅ **Clear Architecture**: File I/O vs. data processing cleanly separated
✅ **Flexibility**: Works with any data source
✅ **Proper Logging**: Comprehensive logging at every step
✅ **Type Safety**: Full type hints throughout
✅ **Zero Duplication**: One generic method instead of three duplicate ones

## Migration from Original Code

The old code mixed file I/O with processing. The new code separates them:

| Old Code | New Code |
|----------|----------|
| `process_subject_summary_file(file_path, file_name, mapping_df)` | **Databricks**: `df = read_csv(file_path)`<br>**DataCurator**: `curator.standardize_subject_summary(df, filename)` |
| Hardcoded file reading in class methods | File reading in Databricks notebook or local runner |
| Can't run locally without dbutils | `local_runner.py` works anywhere |
| No unit tests possible | Full unit test suite |

## Future Enhancements

1. **Configuration Management**: Move MAPPING_CONFIG to external JSON/YAML
2. **Data Validation**: Add schema validation using Pydantic
3. **Parallel Processing**: Process multiple files in parallel locally
4. **Caching**: Add caching for mapping files
5. **Performance Metrics**: Add timing and performance logging
