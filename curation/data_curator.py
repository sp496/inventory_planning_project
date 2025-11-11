"""
Data Curator Module

This module contains all pandas-based data processing logic for clinical inventory curation.
It accepts DataFrames as input, making it fully portable and testable locally or in Databricks.

Key Design:
- Primary methods accept DataFrames (not file paths)
- Databricks notebook handles file I/O and passes DataFrames
- Can be run locally by reading CSV files and passing DataFrames
"""

import re
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import pandas as pd


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Constants:
    """Constants used throughout the data curation process."""
    STUDY_PROTOCOL_PATTERN = r'GS-US-\d+-\d+'
    DATE_FOLDER_FORMAT = "%Y%m%d"
    INPUT_DATE_FORMATS = ['%d-%b-%Y', '%d %b %Y']  # Support multiple date formats
    OUTPUT_DATE_FORMAT = '%Y-%m-%d'
    TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'


class DataCurator:
    """
    Handles all pandas-based data processing for clinical inventory curation.

    This class works with DataFrames as input, making it portable and testable.
    It does NOT handle file I/O - that's the responsibility of the caller.
    """

    def __init__(self, mapping_df: Optional[pd.DataFrame] = None):
        """
        Initialize the DataCurator.

        Args:
            mapping_df: DataFrame containing column mappings for standardization
        """
        self.mapping_df = mapping_df
        logger.info("DataCurator initialized")

    # ========================================================================
    # Static Utility Methods (can be used without instance)
    # ========================================================================

    @staticmethod
    def extract_study_protocol(filename: str) -> str:
        """
        Extract Study Protocol from filename.

        Args:
            filename: Filename containing study protocol (e.g., "Gilead GS-US-592-6173_Subject Summary...")

        Returns:
            Study protocol string (e.g., "GS-US-592-6173")

        Raises:
            ValueError: If study protocol pattern not found
        """
        match = re.search(Constants.STUDY_PROTOCOL_PATTERN, filename)
        if not match:
            raise ValueError(f"Could not extract Study Protocol from filename: {filename}")

        study_protocol = match.group(0)
        logger.debug(f"Extracted study protocol: {study_protocol} from {filename}")
        return study_protocol

    @staticmethod
    def remove_rows_with_n_values(df: pd.DataFrame, n: int = 1) -> pd.DataFrame:
        """
        Remove rows that have N or fewer non-null values.

        Args:
            df: Input DataFrame
            n: Threshold for minimum non-null values

        Returns:
            DataFrame with sparse rows removed
        """
        non_null_counts = df.notna().sum(axis=1)
        df_filtered = df[non_null_counts > n].reset_index(drop=True)

        removed_count = len(df) - len(df_filtered)
        if removed_count > 0:
            logger.info(f"Removed {removed_count} rows with {n} or fewer values")

        return df_filtered

    @staticmethod
    def convert_date_columns(df: pd.DataFrame,
                            date_columns: List[str],
                            input_formats: List[str] = None) -> pd.DataFrame:
        """
        Convert date columns to datetime format, trying multiple formats.

        This method handles columns that may contain dates in different formats
        by trying each format and combining the results.

        Args:
            df: Input DataFrame
            date_columns: List of column names to convert
            input_formats: List of possible input date format strings. If None, uses Constants.INPUT_DATE_FORMATS

        Returns:
            DataFrame with converted date columns
        """
        if input_formats is None:
            input_formats = Constants.INPUT_DATE_FORMATS

        df = df.copy()

        for col in date_columns:
            if col not in df.columns:
                continue

            # Start with all NaT (Not a Time)
            result = pd.Series([pd.NaT] * len(df), index=df.index)
            total_converted = 0

            # Try each format and fill in successfully converted values
            for fmt in input_formats:
                try:
                    # Try converting with this format
                    converted = pd.to_datetime(df[col], format=fmt, errors='coerce')

                    # For rows that are still NaT in result, try to use this format's result
                    mask = result.isna() & converted.notna()
                    result[mask] = converted[mask]

                    newly_converted = mask.sum()
                    if newly_converted > 0:
                        logger.debug(f"Format '{fmt}' converted {newly_converted} values in column '{col}'")
                        total_converted += newly_converted

                except Exception as e:
                    logger.debug(f"Format {fmt} failed for column {col}: {str(e)}")
                    continue

            # Assign the result back to the dataframe
            df[col] = result

            if total_converted > 0:
                logger.info(f"Converted date column '{col}': {total_converted}/{len(df)} values successfully parsed")
            else:
                logger.warning(f"Could not convert any values in date column '{col}' with provided formats: {input_formats}")

        return df

    @staticmethod
    def add_metadata_columns(df: pd.DataFrame,
                           date_folder: str,
                           source_file: str) -> pd.DataFrame:
        """
        Add metadata columns to DataFrame.

        Args:
            df: Input DataFrame
            date_folder: Date folder string (e.g., "20251106")
            source_file: Source filename

        Returns:
            DataFrame with metadata columns added
        """
        df = df.copy()

        df['extract_date'] = pd.to_datetime(
            date_folder,
            format=Constants.DATE_FOLDER_FORMAT
        ).strftime(Constants.OUTPUT_DATE_FORMAT)

        df['processed_timestamp'] = datetime.now().strftime(Constants.TIMESTAMP_FORMAT)
        df['source_file'] = source_file

        logger.debug(f"Added metadata: extract_date={df['extract_date'].iloc[0]}, "
                    f"source_file={source_file}")

        return df

    # ========================================================================
    # Column Mapping Methods
    # ========================================================================

    def create_column_mapping(self, study_protocol: str) -> Dict[str, str]:
        """
        Create column mapping dictionary for a specific study protocol.

        Args:
            study_protocol: Study protocol identifier

        Returns:
            Dictionary mapping original column names to standardized names

        Raises:
            ValueError: If mapping_df not set or study protocol not found
        """
        if self.mapping_df is None:
            raise ValueError("mapping_df not set. Initialize DataCurator with mapping DataFrame.")

        if study_protocol not in self.mapping_df.columns:
            raise ValueError(
                f"Study Protocol '{study_protocol}' not found in mapping file. "
                f"Available protocols: {list(self.mapping_df.columns[1:])}"
            )

        column_mapping = {}

        for _, row in self.mapping_df.iterrows():
            std_col = row['Column Header']
            orig_col = row[study_protocol]

            # Skip if the original column is blank/null
            if pd.notna(orig_col) and str(orig_col).strip():
                column_mapping[str(orig_col).strip()] = std_col

        logger.debug(f"Created column mapping with {len(column_mapping)} columns for {study_protocol}")
        return column_mapping

    # ========================================================================
    # Core Processing Methods (accept DataFrames)
    # ========================================================================

    def standardize_subject_summary(self,
                                    df: pd.DataFrame,
                                    filename: str) -> Tuple[pd.DataFrame, str]:
        """
        Standardize a Subject Summary DataFrame based on header mapping.

        Args:
            df: Input DataFrame (already read from CSV)
            filename: Original filename to extract Study Protocol from

        Returns:
            Tuple of (standardized DataFrame, study_protocol)

        Raises:
            ValueError: If mapping cannot be applied
        """
        if self.mapping_df is None:
            raise ValueError("mapping_df not set. Initialize DataCurator with mapping DataFrame.")

        df = df.copy()

        # Extract Study Protocol from filename
        study_protocol = self.extract_study_protocol(filename)

        # Add Study Protocol column
        df['Study Protocol'] = study_protocol
        logger.debug(f"Added Study Protocol column: {study_protocol}")

        # Get column mapping
        column_mapping = self.create_column_mapping(study_protocol)

        # Get standardized column names
        standardized_columns = self.mapping_df['Column Header'].tolist()

        # Rename columns based on mapping
        columns_to_rename = {}
        for col in df.columns:
            if col == 'Study Protocol':
                continue
            col_stripped = col.strip()
            if col_stripped in column_mapping:
                standardized_name = column_mapping[col_stripped]
                if col != standardized_name:
                    columns_to_rename[col] = standardized_name

        if columns_to_rename:
            df = df.rename(columns=columns_to_rename)
            logger.info(f"Renamed {len(columns_to_rename)} columns")

        # Create standardized dataframe with all standard columns
        final_columns = ['Study Protocol'] + standardized_columns
        df_standardized = pd.DataFrame(index=df.index)

        # Add Study Protocol column first
        df_standardized['Study Protocol'] = study_protocol

        # Add each standardized column
        for std_col in standardized_columns:
            if std_col in df.columns:
                df_standardized[std_col] = df[std_col]
            else:
                df_standardized[std_col] = None

        logger.info(f"Standardized Subject Summary: {df_standardized.shape[0]} rows, "
                   f"{df_standardized.shape[1]} columns")

        return df_standardized, study_protocol

    def add_study_protocol_column(self,
                                  df: pd.DataFrame,
                                  filename: str) -> Tuple[pd.DataFrame, str]:
        """
        Add Study Protocol column to a DataFrame (for depot, site, supply method files).

        Args:
            df: Input DataFrame
            filename: Filename to extract study protocol from

        Returns:
            Tuple of (DataFrame with Study Protocol column, study_protocol)
        """
        df = df.copy()

        # Extract study protocol
        study_protocol = self.extract_study_protocol(filename)

        # Add Study Protocol column at the beginning
        df.insert(0, 'Study Protocol', study_protocol)

        logger.info(f"Added Study Protocol '{study_protocol}' to DataFrame: {df.shape}")

        return df, study_protocol

    # ========================================================================
    # Batch Processing Methods
    # ========================================================================

    def process_subject_summary_batch(self,
                                     dataframes: List[Tuple[pd.DataFrame, str]],
                                     date_folder: str,
                                     column_mapping: Dict[str, str],
                                     date_columns: List[str]) -> Optional[pd.DataFrame]:
        """
        Process multiple Subject Summary DataFrames and combine them.

        Args:
            dataframes: List of (DataFrame, filename) tuples
            date_folder: Date folder string (e.g., "20251106")
            column_mapping: Dictionary to rename columns
            date_columns: List of date column names to convert

        Returns:
            Combined and processed DataFrame, or None if no dataframes provided
        """
        if not dataframes:
            logger.warning(f"No Subject Summary dataframes provided for {date_folder}")
            return None

        processed_dfs = []

        for df, filename in dataframes:
            try:
                # Standardize the dataframe
                standardized_df, study_protocol = self.standardize_subject_summary(df, filename)

                # Add metadata
                standardized_df = self.add_metadata_columns(standardized_df, date_folder, filename)

                processed_dfs.append(standardized_df)
                logger.info(f"Processed Subject Summary: {filename}")

            except Exception as e:
                logger.error(f"Error processing Subject Summary {filename}: {str(e)}", exc_info=True)
                continue

        if not processed_dfs:
            logger.error(f"Failed to process any Subject Summary files for {date_folder}")
            return None

        # Combine all files
        combined_df = pd.concat(processed_dfs, ignore_index=True)
        logger.info(f"Combined {len(processed_dfs)} Subject Summary files: {combined_df.shape}")

        # Rename columns to match database schema
        combined_df = combined_df.rename(columns=column_mapping)

        # Convert date columns
        combined_df = self.convert_date_columns(combined_df, date_columns)

        return combined_df

    def process_generic_batch(self,
                             dataframes: List[Tuple[pd.DataFrame, str]],
                             date_folder: str,
                             file_type: str,
                             column_mapping: Dict[str, str],
                             date_columns: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Process multiple generic DataFrames (depot, site, supply method) and combine them.

        Args:
            dataframes: List of (DataFrame, filename) tuples
            date_folder: Date folder string
            file_type: Type of file for logging
            column_mapping: Dictionary to rename columns
            date_columns: Optional list of date column names to convert

        Returns:
            Combined and processed DataFrame, or None if no dataframes provided
        """
        if not dataframes:
            logger.warning(f"No {file_type} dataframes provided for {date_folder}")
            return None

        processed_dfs = []

        for df, filename in dataframes:
            try:
                # Add study protocol column
                df_with_protocol, study_protocol = self.add_study_protocol_column(df, filename)

                # Add metadata
                df_with_protocol = self.add_metadata_columns(df_with_protocol, date_folder, filename)

                processed_dfs.append(df_with_protocol)
                logger.info(f"Processed {file_type}: {filename}")

            except Exception as e:
                logger.error(f"Error processing {file_type} {filename}: {str(e)}", exc_info=True)
                continue

        if not processed_dfs:
            logger.error(f"Failed to process any {file_type} files for {date_folder}")
            return None

        # Combine all files
        combined_df = pd.concat(processed_dfs, ignore_index=True)
        logger.info(f"Combined {len(processed_dfs)} {file_type} files: {combined_df.shape}")

        # Rename columns to match database schema
        combined_df = combined_df.rename(columns=column_mapping)

        # Convert date columns if specified
        if date_columns:
            combined_df = self.convert_date_columns(combined_df, date_columns)

        return combined_df

    # ========================================================================
    # Convenience Methods for Local Debugging (accept file paths)
    # ========================================================================

    def process_subject_summary_batch_from_files(self,
                                                 file_paths: List[str],
                                                 date_folder: str,
                                                 column_mapping: Dict[str, str],
                                                 date_columns: List[str]) -> Optional[pd.DataFrame]:
        """
        Process multiple Subject Summary CSV files (convenience method for local debugging).

        This method reads CSV files from disk and processes them. Use this for local debugging.
        In Databricks, use process_subject_summary_batch() and pass DataFrames directly.

        Args:
            file_paths: List of file paths to CSV files
            date_folder: Date folder string (e.g., "20251106")
            column_mapping: Dictionary to rename columns
            date_columns: List of date column names to convert

        Returns:
            Combined and processed DataFrame, or None if no files processed successfully

        Example:
            >>> curator = DataCurator(mapping_df=mapping_df)
            >>> result = curator.process_subject_summary_batch_from_files(
            ...     file_paths=['./data/file1.csv', './data/file2.csv'],
            ...     date_folder='20251106',
            ...     column_mapping={"Study Protocol": "study_protocol", ...},
            ...     date_columns=["date_randomized"]
            ... )
        """
        if not file_paths:
            logger.warning("No file paths provided")
            return None

        logger.info(f"Processing {len(file_paths)} Subject Summary files from disk")

        # Read CSV files into DataFrames
        dataframes = []
        for file_path in file_paths:
            try:
                df = read_dynamic_csv(file_path)
                filename = file_path.split('/')[-1]  # Extract filename from path
                dataframes.append((df, filename))
                logger.info(f"✓ Loaded {filename}: {df.shape}")
            except Exception as e:
                logger.error(f"✗ Error loading {file_path}: {str(e)}")
                continue

        if not dataframes:
            logger.error("Failed to load any files")
            return None

        # Process DataFrames using the existing method
        return self.process_subject_summary_batch(
            dataframes=dataframes,
            date_folder=date_folder,
            column_mapping=column_mapping,
            date_columns=date_columns
        )

    def process_generic_batch_from_files(self,
                                         file_paths: List[str],
                                         date_folder: str,
                                         file_type: str,
                                         column_mapping: Dict[str, str],
                                         date_columns: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Process multiple generic CSV files (convenience method for local debugging).

        This method reads CSV files from disk and processes them. Use this for local debugging.
        In Databricks, use process_generic_batch() and pass DataFrames directly.

        Args:
            file_paths: List of file paths to CSV files
            date_folder: Date folder string
            file_type: Type of file for logging (e.g., "depot", "site")
            column_mapping: Dictionary to rename columns
            date_columns: Optional list of date column names to convert

        Returns:
            Combined and processed DataFrame, or None if no files processed successfully

        Example:
            >>> curator = DataCurator(mapping_df=mapping_df)
            >>> result = curator.process_generic_batch_from_files(
            ...     file_paths=['./data/depot1.csv', './data/depot2.csv'],
            ...     date_folder='20251106',
            ...     file_type='depot',
            ...     column_mapping={"Study Protocol": "study_protocol", ...},
            ...     date_columns=["fp_expiry_date"]
            ... )
        """
        if not file_paths:
            logger.warning(f"No file paths provided for {file_type}")
            return None

        logger.info(f"Processing {len(file_paths)} {file_type} files from disk")

        # Read CSV files into DataFrames
        dataframes = []
        for file_path in file_paths:
            try:
                df = read_dynamic_csv(file_path)
                filename = file_path.split('/')[-1]  # Extract filename from path
                dataframes.append((df, filename))
                logger.info(f"✓ Loaded {filename}: {df.shape}")
            except Exception as e:
                logger.error(f"✗ Error loading {file_path}: {str(e)}")
                continue

        if not dataframes:
            logger.error(f"Failed to load any {file_type} files")
            return None

        # Process DataFrames using the existing method
        return self.process_generic_batch(
            dataframes=dataframes,
            date_folder=date_folder,
            file_type=file_type,
            column_mapping=column_mapping,
            date_columns=date_columns
        )


# ========================================================================
# Convenience Functions for Local File Reading
# ========================================================================

def read_dynamic_csv(filepath: str, max_rows: int = 10) -> pd.DataFrame:
    """
    Read CSV file with dynamic header row detection.

    Finds the first row where all cells are non-empty and uses it as the header.
    This is a convenience function for local testing.

    Args:
        filepath: Path to the CSV file
        max_rows: Maximum number of rows to search for header

    Returns:
        DataFrame with data

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If no valid header found
    """
    logger.info(f"Reading CSV with dynamic header detection: {filepath}")

    with open(filepath, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= max_rows:
                raise ValueError(f"No valid header found in first {max_rows} rows of {filepath}")

            cells = [c.strip() for c in line.split(',')]

            # Check if all cells are non-empty and has multiple columns
            if len(cells) > 1 and all(cells):
                logger.info(f"Found header at line {i} in {filepath}")
                df = pd.read_csv(filepath, dtype=str, encoding='utf-8', skiprows=i)
                logger.info(f"Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")
                return df

    raise ValueError(f"No fully populated header line found in {filepath}")


def load_excel_mapping(excel_path: str, sheet_name: str = 'Header') -> pd.DataFrame:
    """
    Load column mapping from an Excel file.

    This is a convenience function for loading mapping files.

    Args:
        excel_path: Path to the Excel file containing mappings
        sheet_name: Name of the sheet to read

    Returns:
        DataFrame with mapping data where:
        - First column 'Column Header' contains standardized column names
        - Each subsequent column is a Study Protocol with original column names

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If 'Column Header' column not found
    """
    logger.info(f"Loading Excel mapping from: {excel_path}, sheet: {sheet_name}")

    mapping_df = pd.read_excel(excel_path, sheet_name=sheet_name, engine='openpyxl')

    # Verify 'Column Header' column exists
    if 'Column Header' not in mapping_df.columns:
        raise ValueError(
            f"'Column Header' column not found in mapping file. "
            f"Found columns: {list(mapping_df.columns)}"
        )

    logger.info(f"Mapping loaded: {len(mapping_df)} rows, "
               f"{len(mapping_df.columns)-1} study protocols")

    return mapping_df
