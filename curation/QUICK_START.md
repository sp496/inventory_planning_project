# Quick Local Debugging Guide

## Super Simple Usage

Just pass file paths directly to DataCurator - no need to read files manually!

```python
from curation.data_curator import DataCurator, load_excel_mapping

# 1. Load mapping
mapping_df = load_excel_mapping('./test_data/header_mapping.xlsx')
curator = DataCurator(mapping_df=mapping_df)

# 2. Process Subject Summary files - just pass file paths!
result = curator.process_subject_summary_batch_from_files(
    file_paths=[
        './test_data/GS-US-592-6173_Subject_Summary.csv',
        './test_data/GS-US-592-6789_Subject_Summary.csv'
    ],
    date_folder='20251106',
    column_mapping={
        "Study Protocol": "study_protocol",
        "Site ID": "site_id",
        "Country": "country",
        # ... rest of mapping
    },
    date_columns=["date_randomized", "date_crossover_enrolled"]
)

# 3. Inspect or save result
print(result.head())
result.to_csv('./output.csv', index=False)
```

## Even Simpler: Use quick_debug.py

Edit the file paths in `curation/quick_debug.py` and run:

```bash
cd curation
python quick_debug.py
```

That's it! It will:
- Load your mapping file
- Process all your CSV files
- Save results to `./output/`
- Show you exactly where any errors occur

## Debugging Workflow

When you get an error in Databricks:

```bash
# 1. Download problematic CSV from Databricks
dbfs cp dbfs:/mnt/data/.../problem_file.csv ./test_data/

# 2. Download mapping
dbfs cp dbfs:/mnt/config/header_mapping.xlsx ./test_data/

# 3. Edit quick_debug.py to point to your files
# Change:
SUBJECT_FILES = [
    "./test_data/problem_file.csv"  # Your file here
]

# 4. Run it
python curation/quick_debug.py

# 5. You'll see the same error with full traceback!

# 6. Debug with your favorite tool:
# - VS Code debugger
# - PyCharm debugger
# - Add: import pdb; pdb.set_trace()
```

## Method Reference

### For Subject Summary files
```python
result = curator.process_subject_summary_batch_from_files(
    file_paths=['file1.csv', 'file2.csv'],
    date_folder='20251106',
    column_mapping=MAPPING_CONFIG["subject"]["column_mapping"],
    date_columns=MAPPING_CONFIG["subject"]["date_columns"]
)
```

### For Depot/Site/Supply Method files
```python
result = curator.process_generic_batch_from_files(
    file_paths=['depot1.csv', 'depot2.csv'],
    date_folder='20251106',
    file_type='depot',  # or 'site', 'supply_method', etc.
    column_mapping=MAPPING_CONFIG["depot"]["column_mapping"],
    date_columns=MAPPING_CONFIG["depot"]["date_columns"]
)
```

## Minimal Example (Copy-Paste Ready)

```python
#!/usr/bin/env python3
from curation.data_curator import DataCurator, load_excel_mapping

# Configure
MAPPING_FILE = "./test_data/header_mapping.xlsx"
CSV_FILES = ["./test_data/GS-US-592-6173_Subject_Summary.csv"]
DATE_FOLDER = "20251106"

COLUMN_MAPPING = {
    "Study Protocol": "study_protocol",
    "Site ID": "site_id",
    "Country": "country",
    # Add more mappings as needed
}

DATE_COLUMNS = ["date_randomized"]

# Load and process
mapping_df = load_excel_mapping(MAPPING_FILE)
curator = DataCurator(mapping_df=mapping_df)

result = curator.process_subject_summary_batch_from_files(
    file_paths=CSV_FILES,
    date_folder=DATE_FOLDER,
    column_mapping=COLUMN_MAPPING,
    date_columns=DATE_COLUMNS
)

print(result.head())
result.to_csv('./output.csv', index=False)
```

## Tips

1. **Use absolute paths** if having issues with relative paths
2. **Check your mapping file** has the 'Column Header' column
3. **Ensure CSV files** have the study protocol pattern in filename (e.g., GS-US-592-6173)
4. **Add logging** to see exactly what's happening:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

5. **Debug interactively** in Python/IPython:
   ```python
   from curation.data_curator import *

   mapping_df = load_excel_mapping('./test_data/header_mapping.xlsx')
   curator = DataCurator(mapping_df=mapping_df)

   # Now experiment interactively!
   df = read_dynamic_csv('./test_data/file.csv')
   result, protocol = curator.standardize_subject_summary(df, 'filename.csv')
   ```
