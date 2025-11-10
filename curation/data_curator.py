"""
Data Curator Module

This module contains all pandas-based data processing logic for clinical inventory curation.
It is designed to be independent of Databricks so it can be tested and used in any environment.
"""

import os
import re
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
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
    INPUT_DATE_FORMAT = '%d-%b-%Y'
    OUTPUT_DATE_FORMAT = '%Y-%m-%d'
    TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
    MAX_HEADER_SEARCH_ROWS = 10


class DataCurator:
    """
    Handles all pandas-based data processing for clinical inventory curation.

    This class is independent of Databricks and can be used in any Python environment.
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
    # File Reading Methods
    # ========================================================================

    @staticmethod
    def read_dynamic_csv(filepath: str, max_rows: int = Constants.MAX_HEADER_SEARCH_ROWS) -> pd.DataFrame:
        """
        Read CSV file with dynamic header row detection.

        Finds the first row where all cells are non-empty and uses it as the header.

        Args:
            filepath: Path to the CSV file
            max_rows: Maximum number of rows to search for header

        Returns:
            DataFrame with data

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If no valid header found
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= max_rows:
                        raise ValueError(f"No valid header found in first {max_rows} rows of {filepath}")

                    cells = [c.strip() for c in line.split(',')]

                    # Check if all cells are non-empty and has multiple columns
                    if len(cells) > 1 and all(cells):
                        logger.info(f"Found header at line {i} in {filepath}")
                        df = pd.read_csv(filepath, dtype=str, encoding='utf-8', skiprows=i)
                        return df

            raise ValueError(f"No fully populated header line found in {filepath}")

        except Exception as e:
            logger.error(f"Error reading CSV file {filepath}: {str(e)}")
            raise

    @staticmethod
    def load_excel_mapping(excel_path: str, sheet_name: str = 'Header') -> pd.DataFrame:
        """
        Load column mapping from an Excel file.

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
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel file not found: {excel_path}")

        logger.info(f"Loading mapping from: {excel_path}, sheet: {sheet_name}")

        try:
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

        except Exception as e:
            logger.error(f"Error loading Excel mapping: {str(e)}")
            raise

    # ========================================================================
    # Data Extraction and Parsing Methods
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

    # ========================================================================
    # Data Transformation Methods
    # ========================================================================

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

    def standardize_dataframe(self, source_df: pd.DataFrame, filename: str) -> Tuple[pd.DataFrame, str]:
        """
        Standardize a dataframe based on header mapping.

        Args:
            source_df: pandas DataFrame with source data
            filename: Original filename to extract Study Protocol from

        Returns:
            Tuple of (standardized DataFrame, study_protocol)

        Raises:
            ValueError: If mapping cannot be applied
        """
        if self.mapping_df is None:
            raise ValueError("mapping_df not set. Initialize DataCurator with mapping DataFrame.")

        df = source_df.copy()

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

        logger.info(f"Standardized dataframe: {df_standardized.shape[0]} rows, {df_standardized.shape[1]} columns")

        return df_standardized, study_protocol

    @staticmethod
    def convert_date_columns(df: pd.DataFrame, date_columns: List[str],
                            input_format: str = Constants.INPUT_DATE_FORMAT) -> pd.DataFrame:
        """
        Convert date columns to datetime format.

        Args:
            df: Input DataFrame
            date_columns: List of column names to convert
            input_format: Input date format string

        Returns:
            DataFrame with converted date columns
        """
        df = df.copy()

        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format=input_format, errors='coerce')
                logger.debug(f"Converted date column: {col}")

        return df

    @staticmethod
    def add_metadata_columns(df: pd.DataFrame, date_folder: str,
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

        df['extract_date'] = pd.to_datetime(date_folder, format=Constants.DATE_FOLDER_FORMAT).strftime(
            Constants.OUTPUT_DATE_FORMAT
        )
        df['processed_timestamp'] = datetime.now().strftime(Constants.TIMESTAMP_FORMAT)
        df['source_file'] = source_file

        logger.debug(f"Added metadata columns: extract_date={df['extract_date'].iloc[0]}, "
                    f"source_file={source_file}")

        return df

    # ========================================================================
    # File Processing Methods
    # ========================================================================

    def process_subject_summary_file(self, file_path: str,
                                    file_name: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Read and standardize a Subject Summary CSV file.

        Args:
            file_path: Full path to the CSV file
            file_name: Name of the file

        Returns:
            Tuple of (standardized DataFrame, study_protocol) or (None, None) if processing fails
        """
        try:
            logger.info(f"Processing Subject Summary file: {file_name}")

            # Convert dbfs path to local path if needed
            local_path = file_path.replace("dbfs:", "/dbfs")

            # Read CSV file
            df = self.read_dynamic_csv(local_path)
            logger.info(f"Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")

            # Apply standardization
            standardized_df, study_protocol = self.standardize_dataframe(df, file_name)

            logger.info(f"Successfully standardized Subject Summary file. "
                       f"Study Protocol: {study_protocol}, Output shape: {standardized_df.shape}")

            return standardized_df, study_protocol

        except Exception as e:
            logger.error(f"Error processing Subject Summary file {file_name}: {str(e)}", exc_info=True)
            return None, None

    def process_generic_file(self, file_path: str, file_name: str,
                            file_type: str = "generic") -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Read and process a generic CSV file (depot, site, supply method, etc.).

        Args:
            file_path: Full path to the CSV file
            file_name: Name of the file
            file_type: Type of file for logging purposes

        Returns:
            Tuple of (DataFrame with Study Protocol column, study_protocol) or (None, None) if processing fails
        """
        try:
            logger.info(f"Processing {file_type} file: {file_name}")

            # Convert dbfs path to local path if needed
            local_path = file_path.replace("dbfs:", "/dbfs")

            # Read CSV file
            df = self.read_dynamic_csv(local_path)
            logger.info(f"Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")

            # Extract study protocol
            study_protocol = self.extract_study_protocol(file_name)

            # Add Study Protocol column at the beginning
            df.insert(0, 'Study Protocol', study_protocol)

            logger.info(f"Successfully processed {file_type} file. "
                       f"Study Protocol: {study_protocol}, Output shape: {df.shape}")

            return df, study_protocol

        except Exception as e:
            logger.error(f"Error processing {file_type} file {file_name}: {str(e)}", exc_info=True)
            return None, None

    # ========================================================================
    # Batch Processing Methods
    # ========================================================================

    def process_subject_summary_batch(self, file_list: List[Tuple[str, str]],
                                     date_folder: str,
                                     column_mapping: Dict[str, str],
                                     date_columns: List[str]) -> Optional[pd.DataFrame]:
        """
        Process multiple Subject Summary files and combine them.

        Args:
            file_list: List of (file_path, file_name) tuples
            date_folder: Date folder string (e.g., "20251106")
            column_mapping: Dictionary to rename columns
            date_columns: List of date column names to convert

        Returns:
            Combined and processed DataFrame, or None if no files processed successfully
        """
        if not file_list:
            logger.warning(f"No Subject Summary files found for {date_folder}")
            return None

        processed_dfs = []

        for file_path, file_name in file_list:
            df, study_protocol = self.process_subject_summary_file(file_path, file_name)

            if df is not None:
                df = self.add_metadata_columns(df, date_folder, file_name)
                processed_dfs.append(df)

        if not processed_dfs:
            logger.error(f"Failed to process any Subject Summary files for {date_folder}")
            return None

        # Combine all files
        combined_df = pd.concat(processed_dfs, ignore_index=True)
        logger.info(f"Combined {len(processed_dfs)} Subject Summary files: {combined_df.shape}")

        # Rename columns
        combined_df = combined_df.rename(columns=column_mapping)

        # Convert date columns
        combined_df = self.convert_date_columns(combined_df, date_columns)

        return combined_df

    def process_generic_batch(self, file_list: List[Tuple[str, str]],
                             date_folder: str,
                             file_type: str,
                             column_mapping: Dict[str, str],
                             date_columns: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Process multiple generic files (depot, site, supply method) and combine them.

        Args:
            file_list: List of (file_path, file_name) tuples
            date_folder: Date folder string
            file_type: Type of file for logging
            column_mapping: Dictionary to rename columns
            date_columns: Optional list of date column names to convert

        Returns:
            Combined and processed DataFrame, or None if no files processed successfully
        """
        if not file_list:
            logger.warning(f"No {file_type} files found for {date_folder}")
            return None

        processed_dfs = []

        for file_path, file_name in file_list:
            df, study_protocol = self.process_generic_file(file_path, file_name, file_type)

            if df is not None:
                df = self.add_metadata_columns(df, date_folder, file_name)
                processed_dfs.append(df)

        if not processed_dfs:
            logger.error(f"Failed to process any {file_type} files for {date_folder}")
            return None

        # Combine all files
        combined_df = pd.concat(processed_dfs, ignore_index=True)
        logger.info(f"Combined {len(processed_dfs)} {file_type} files: {combined_df.shape}")

        # Rename columns
        combined_df = combined_df.rename(columns=column_mapping)

        # Convert date columns if specified
        if date_columns:
            combined_df = self.convert_date_columns(combined_df, date_columns)

        return combined_df

    # ========================================================================
    # Treatment Mapping Methods
    # ========================================================================

    @staticmethod
    def process_treatment_mapping(excel_path: str, sheet_name: str,
                                 column_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Load and process treatment group mapping from Excel.

        Args:
            excel_path: Path to Excel file
            sheet_name: Sheet name to read
            column_mapping: Dictionary to rename columns

        Returns:
            Processed DataFrame with renamed columns
        """
        logger.info(f"Loading treatment mapping from {excel_path}")

        try:
            tgm_df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype='str', engine='openpyxl')

            # Rename columns
            tgm_df = tgm_df.rename(columns=column_mapping)

            logger.info(f"Loaded treatment mapping: {tgm_df.shape[0]} rows, {tgm_df.shape[1]} columns")

            return tgm_df

        except Exception as e:
            logger.error(f"Error loading treatment mapping: {str(e)}")
            raise
