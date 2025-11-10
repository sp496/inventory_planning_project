"""
Unit tests for DataCurator class

These tests demonstrate how the separated pandas logic can be tested
without requiring a Databricks environment.
"""

import pytest
import pandas as pd
from datetime import datetime
import tempfile
import os
from data_curator import DataCurator, Constants


class TestConstants:
    """Test Constants class."""

    def test_study_protocol_pattern(self):
        """Test study protocol regex pattern."""
        import re
        pattern = Constants.STUDY_PROTOCOL_PATTERN

        # Valid patterns
        assert re.search(pattern, "GS-US-592-6173")
        assert re.search(pattern, "Gilead GS-US-123-4567_Subject Summary.csv")

        # Invalid patterns
        assert not re.search(pattern, "GS-US-ABC-6173")
        assert not re.search(pattern, "Random text")


class TestDataCurator:
    """Test DataCurator class methods."""

    @pytest.fixture
    def sample_mapping_df(self):
        """Create a sample mapping DataFrame for testing."""
        return pd.DataFrame({
            'Column Header': ['Site ID', 'Country', 'Subject Number'],
            'GS-US-592-6173': ['Site', 'Country', 'Subject']
        })

    @pytest.fixture
    def curator_with_mapping(self, sample_mapping_df):
        """Create DataCurator instance with mapping."""
        return DataCurator(mapping_df=sample_mapping_df)

    @pytest.fixture
    def sample_csv_file(self):
        """Create a temporary CSV file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            # Write a CSV with header on line 2
            f.write(",,\n")  # Empty first line
            f.write("Site,Country,Subject\n")
            f.write("101,USA,12345\n")
            f.write("102,Canada,12346\n")
            temp_path = f.name

        yield temp_path

        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)

    def test_extract_study_protocol_valid(self):
        """Test extracting valid study protocol from filename."""
        filename = "Gilead GS-US-592-6173_Subject Summary.csv"
        protocol = DataCurator.extract_study_protocol(filename)
        assert protocol == "GS-US-592-6173"

    def test_extract_study_protocol_invalid(self):
        """Test extracting study protocol from invalid filename."""
        filename = "Invalid_File_Name.csv"
        with pytest.raises(ValueError, match="Could not extract Study Protocol"):
            DataCurator.extract_study_protocol(filename)

    def test_remove_rows_with_n_values(self):
        """Test removing sparse rows."""
        df = pd.DataFrame({
            'A': [1, None, 3, None],
            'B': [None, None, 4, 5],
            'C': [None, None, None, 6]
        })

        result = DataCurator.remove_rows_with_n_values(df, n=1)

        # Only rows with more than 1 non-null value should remain
        assert len(result) == 2  # Rows 2 and 3 (0-indexed)
        assert list(result['A'].dropna()) == [3]

    def test_read_dynamic_csv(self, sample_csv_file):
        """Test reading CSV with dynamic header detection."""
        df = DataCurator.read_dynamic_csv(sample_csv_file)

        assert len(df) == 2
        assert list(df.columns) == ['Site', 'Country', 'Subject']
        assert df['Site'].iloc[0] == '101'
        assert df['Country'].iloc[1] == 'Canada'

    def test_read_dynamic_csv_file_not_found(self):
        """Test reading non-existent CSV file."""
        with pytest.raises(FileNotFoundError):
            DataCurator.read_dynamic_csv('/nonexistent/file.csv')

    def test_create_column_mapping(self, curator_with_mapping):
        """Test creating column mapping dictionary."""
        mapping = curator_with_mapping.create_column_mapping('GS-US-592-6173')

        assert mapping == {
            'Site': 'Site ID',
            'Country': 'Country',
            'Subject': 'Subject Number'
        }

    def test_create_column_mapping_invalid_protocol(self, curator_with_mapping):
        """Test creating mapping with invalid study protocol."""
        with pytest.raises(ValueError, match="not found in mapping file"):
            curator_with_mapping.create_column_mapping('GS-US-999-9999')

    def test_create_column_mapping_no_mapping_df(self):
        """Test creating mapping without mapping_df set."""
        curator = DataCurator()
        with pytest.raises(ValueError, match="mapping_df not set"):
            curator.create_column_mapping('GS-US-592-6173')

    def test_convert_date_columns(self):
        """Test converting date columns."""
        df = pd.DataFrame({
            'date1': ['01-Jan-2024', '15-Feb-2024'],
            'date2': ['31-Dec-2023', '01-Mar-2024'],
            'value': ['A', 'B']
        })

        result = DataCurator.convert_date_columns(df, ['date1', 'date2'])

        assert pd.api.types.is_datetime64_any_dtype(result['date1'])
        assert pd.api.types.is_datetime64_any_dtype(result['date2'])
        assert result['date1'].iloc[0] == pd.Timestamp('2024-01-01')

    def test_add_metadata_columns(self):
        """Test adding metadata columns."""
        df = pd.DataFrame({'col1': [1, 2, 3]})

        result = DataCurator.add_metadata_columns(
            df,
            date_folder='20251106',
            source_file='test_file.csv'
        )

        assert 'extract_date' in result.columns
        assert 'processed_timestamp' in result.columns
        assert 'source_file' in result.columns

        assert result['extract_date'].iloc[0] == '2025-11-06'
        assert result['source_file'].iloc[0] == 'test_file.csv'

    def test_standardize_dataframe(self, curator_with_mapping):
        """Test standardizing a dataframe."""
        source_df = pd.DataFrame({
            'Site': ['101', '102'],
            'Country': ['USA', 'Canada'],
            'Subject': ['12345', '12346']
        })

        filename = "GS-US-592-6173_Subject_Summary.csv"

        standardized_df, protocol = curator_with_mapping.standardize_dataframe(
            source_df, filename
        )

        assert protocol == 'GS-US-592-6173'
        assert 'Study Protocol' in standardized_df.columns
        assert standardized_df['Study Protocol'].iloc[0] == 'GS-US-592-6173'
        assert 'Site ID' in standardized_df.columns
        assert 'Subject Number' in standardized_df.columns

    def test_process_generic_batch(self, curator_with_mapping, sample_csv_file):
        """Test processing a batch of generic files."""
        # Note: This would require mock file system for full test
        # Here we just test the structure
        file_list = []  # Empty list

        result = curator_with_mapping.process_generic_batch(
            file_list=file_list,
            date_folder='20251106',
            file_type='depot',
            column_mapping={'Site': 'site_id'},
            date_columns=[]
        )

        assert result is None  # Empty file list returns None


class TestIntegration:
    """Integration tests for complete workflows."""

    def test_full_file_processing_workflow(self):
        """Test complete workflow from file to processed dataframe."""
        # Create temporary CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("Site,Country,Depot\n")
            f.write("101,USA,1001\n")
            f.write("102,Canada,1002\n")
            temp_path = f.name

        try:
            # Create curator
            curator = DataCurator()

            # Process file
            df, protocol = curator.process_generic_file(
                file_path=temp_path,
                file_name="GS-US-592-6173_Depot_Inventory.csv",
                file_type="depot"
            )

            assert df is not None
            assert protocol == "GS-US-592-6173"
            assert 'Study Protocol' in df.columns
            assert len(df) == 2
            assert df['Study Protocol'].iloc[0] == "GS-US-592-6173"

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_load_excel_mapping_file_not_found(self):
        """Test loading non-existent Excel file."""
        with pytest.raises(FileNotFoundError):
            DataCurator.load_excel_mapping('/nonexistent/file.xlsx')

    def test_read_csv_no_valid_header(self):
        """Test reading CSV with no valid header."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            # Write only empty lines
            for _ in range(15):
                f.write(",,\n")
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="No valid header found"):
                DataCurator.read_dynamic_csv(temp_path, max_rows=10)
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
