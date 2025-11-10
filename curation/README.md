# Clinical Inventory Data Curation - Refactored

This directory contains the refactored data curation code, separating pandas-based data processing from Databricks-specific operations.

## Structure

```
curation/
├── data_curator.py           # Pure Python/Pandas processing class
├── curate_data_refactored.py # Databricks notebook using DataCurator
├── test_data_curator.py      # Unit tests for DataCurator
└── README.md                 # This file
```

## Key Improvements

### 1. Separation of Concerns
- **`data_curator.py`**: Contains all pandas/Python logic, independent of Databricks
- **`curate_data_refactored.py`**: Databricks-specific operations (mounting, Spark operations)

### 2. Benefits

#### Testability
- Can now unit test pandas logic without Databricks environment
- Mock file operations for faster testing
- Test edge cases easily

#### Maintainability
- Single responsibility: DataCurator handles data processing
- Clear interfaces with type hints
- Comprehensive logging instead of print statements

#### Reusability
- DataCurator can be used in other projects
- Functions are modular and composable
- Configuration-driven approach

#### Code Quality
- Eliminated code duplication (depot, site, supply method processing)
- Proper error handling throughout
- Constants for magic strings
- Docstrings for all public methods

### 3. Code Reduction

Original code: **~1,160 lines** with significant duplication

Refactored code:
- `data_curator.py`: **~500 lines** of clean, reusable code
- `curate_data_refactored.py`: **~480 lines** focused on Databricks operations
- Total: **~980 lines** with better structure and zero duplication

**Savings: ~180 lines + eliminated all duplicate functions**

## Usage

### In Databricks

```python
from curation.data_curator import DataCurator

# Initialize with mapping
mapping_df = DataCurator.load_excel_mapping('/path/to/mapping.xlsx')
curator = DataCurator(mapping_df=mapping_df)

# Process files
df, protocol = curator.process_subject_summary_file(file_path, file_name)

# Batch processing
combined_df = curator.process_subject_summary_batch(
    file_list=file_list,
    date_folder='20251106',
    column_mapping=column_mapping,
    date_columns=date_columns
)
```

### Standalone (Testing/Development)

```python
from curation.data_curator import DataCurator

# Works without Databricks
curator = DataCurator()
df = curator.read_dynamic_csv('/path/to/file.csv')
protocol = curator.extract_study_protocol('GS-US-592-6173_Subject_Summary.csv')
```

## Testing

Run unit tests:
```bash
python -m pytest curation/test_data_curator.py -v
```

## Migration Guide

### Old Code → New Code

**Before:**
```python
def process_depot_file(file_path, file_name):
    # 40 lines of code

def process_site_file(file_path, file_name):
    # 40 lines of code (duplicated)

def process_supplymethod_file(file_path, file_name):
    # 40 lines of code (duplicated)
```

**After:**
```python
# Single generic method
curator.process_generic_file(file_path, file_name, file_type="depot")
curator.process_generic_file(file_path, file_name, file_type="site")
curator.process_generic_file(file_path, file_name, file_type="supply_method")
```

**Before:**
```python
print(f"Processing file: {file_name}")  # No log levels
```

**After:**
```python
logger.info(f"Processing file: {file_name}")  # Proper logging with levels
```

**Before:**
```python
with open("config.json") as f:  # No error handling
    config = json.load(f)
```

**After:**
```python
config = load_config("config.json")  # Comprehensive error handling
```

## Key Features

### DataCurator Class Methods

#### File Reading
- `read_dynamic_csv()` - Smart CSV reading with header detection
- `load_excel_mapping()` - Load Excel mapping files

#### Data Extraction
- `extract_study_protocol()` - Parse study protocol from filename

#### Data Transformation
- `standardize_dataframe()` - Apply column mapping and standardization
- `convert_date_columns()` - Batch date conversion
- `add_metadata_columns()` - Add extract_date, source_file, timestamp

#### Batch Processing
- `process_subject_summary_batch()` - Process multiple subject summary files
- `process_generic_batch()` - Process multiple depot/site/supply method files

#### Utilities
- `remove_rows_with_n_values()` - Clean sparse data

### Constants

All magic strings are centralized:
```python
class Constants:
    STUDY_PROTOCOL_PATTERN = r'GS-US-\d+-\d+'
    DATE_FOLDER_FORMAT = "%Y%m%d"
    INPUT_DATE_FORMAT = '%d-%b-%Y'
    OUTPUT_DATE_FORMAT = '%Y-%m-%d'
```

## Error Handling

All methods include proper error handling:
- File not found errors
- Invalid data format errors
- Missing configuration errors
- Detailed logging with traceback

## Type Hints

All functions include type hints for better IDE support and documentation:
```python
def process_generic_batch(
    self,
    file_list: List[Tuple[str, str]],
    date_folder: str,
    file_type: str,
    column_mapping: Dict[str, str],
    date_columns: Optional[List[str]] = None
) -> Optional[pd.DataFrame]:
    ...
```

## Future Enhancements

1. **Configuration Management**: Move MAPPING_CONFIG to external JSON/YAML
2. **Async Processing**: Add async file reading for better performance
3. **Data Validation**: Add schema validation using Pydantic or Great Expectations
4. **Metrics**: Add performance metrics and monitoring
5. **Caching**: Add caching for mapping files
