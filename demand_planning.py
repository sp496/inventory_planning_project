#!/usr/bin/env python3
"""
Clinical Trial Demand Planning System
======================================
A refactored, debuggable version of the demand planning system that predicts
when patients will need medication refills in clinical trials.

USAGE MODES:
------------

1. Databricks Notebook (with DataFrames):
   ```python
   from demand_planning import run_demand_planning

   # Read from Delta tables
   df_subjects = spark.table("clinical_subject_summary").toPandas()
   df_mapping = spark.table("clinical_treatment_mapping").toPandas()

   # Run demand planning
   df_forecast = run_demand_planning(df_subjects, df_mapping)

   # Convert back to Spark DataFrame if needed
   spark_df = spark.createDataFrame(df_forecast)
   ```

2. Local Debugging (with CSV files):
   ```bash
   python demand_planning.py
   ```
   This will read from CSV files specified in Config class and save output to CSV.

3. Programmatic usage with files:
   ```python
   from demand_planning import DemandPlanningProcessor

   processor = DemandPlanningProcessor()
   df_result = processor.run(
       subject_file="path/to/subjects.csv",
       mapping_file="path/to/mapping.csv",
       output_file="path/to/output.csv"
   )
   ```
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import re
import logging
from dataclasses import dataclass
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# DATA CLASSES FOR TYPE SAFETY
# ============================================================================

@dataclass
class CycleInfo:
    """Represents information about a patient's current cycle"""
    cycle_number: int
    day_number: int
    last_visit_date: pd.Timestamp


@dataclass
class ProjectedVisit:
    """Represents a projected future visit"""
    subject_number: int
    medicine_name: str
    cycle_number: int
    cycle_day: int
    visit_date: str
    medicine_quantity: float
    visit_description: str


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration settings for the demand planning system"""

    # File paths - Update these to match your local setup
    # SUBJECT_SUMMARY_FILE = "test_scenarios/01_simple_single_drug/subject_summary.csv"
    # TREATMENT_MAPPING_FILE = "test_scenarios/01_simple_single_drug/treatment_mapping.csv"
    # OUTPUT_FILE = "test_scenarios/01_simple_single_drug/demand_forecast.csv"

    SUBJECT_SUMMARY_FILE = "clinical_subject_summary.csv"
    TREATMENT_MAPPING_FILE = "treatment_group_mapping.csv"
    OUTPUT_FILE = "demand_forecast.csv"

    # Column mappings for standardization
    SUBJECT_COLUMNS = [
        'study_protocol', 'site_id', 'country', 'parent_depot', 'investigator',
        'subject_number', 'year_of_birth', 'gender', 'tpc', 'date_randomized',
        'date_crossover_enrolled', 'date_crossover_approved',
        'date_crossover_treatment_discontinued', 'subject_status',
        'randomized_treatment', 'last_study_visit_recorded',
        'last_study_visit_date', 'last_study_visit_number',
        'next_min_study_visit_date', 'next_max_study_visit_date',
        'additional_drug_status', 'last_additional_drug_visit_recorded',
        'last_additional_drug_visit_date', 'last_additional_drug_visit_number',
        'next_min_additional_drug_visit_date', 'next_max_additional_drug_visit_date',
        'extract_date', 'source_file', 'processed_timestamp'
    ]

    MAPPING_COLUMNS = [
        'study_protocol', 'randomized_treatment', 'subject_status', 'tpc',
        'study_drug_dispensed', 'additional_study_drug_dispensed',
        'additional_study_drug_prefix', 'country', 'visit_days',
        'dispensing_quantity', 'dispensing_frequency_days', 'max_cycles'
    ]

    # Statuses that indicate a patient has stopped treatment
    EXCLUDED_STATUSES = [
        "Screen Failed",
        "Pre-Screened Failed",
        "Treatment Discontinued",
        "Crossover Treatment Discontinued"
    ]

    # Default values
    DEFAULT_PROJECTION_DAYS = 365  # Project visits for one year ahead (primary constraint)
    # Note: max_cycles from mapping is a secondary constraint (hard cap on cycle numbers)


# ============================================================================
# DATA LOADING AND CLEANING
# ============================================================================

class DataLoader:
    """Handles loading and initial cleaning of data files"""

    @staticmethod
    def prepare_subject_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare and clean subject summary data

        Args:
            df: Raw subject DataFrame

        Returns:
            Cleaned subject DataFrame
        """
        logger.info(f"Preparing subject data ({len(df)} records)")

        # Clean column names first
        df.columns = df.columns.str.strip()

        # Select only required columns if they exist
        available_cols = [col for col in Config.SUBJECT_COLUMNS if col in df.columns]
        df = df[available_cols]

        # Convert date columns
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        logger.info(f"Prepared {len(df)} subject records")
        return df

    @staticmethod
    def prepare_mapping_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare and clean treatment mapping data

        Args:
            df: Raw mapping DataFrame

        Returns:
            Cleaned mapping DataFrame
        """
        logger.info(f"Preparing mapping data ({len(df)} records)")

        # Clean column names first
        df.columns = df.columns.str.strip()

        # Select only required columns if they exist
        available_cols = [col for col in Config.MAPPING_COLUMNS if col in df.columns]
        df = df[available_cols]

        # Handle TPC column variations
        for col in df.columns:
            if "TPC" in col.upper():
                df.rename(columns={col: "tpc"}, inplace=True)
                break

        logger.info(f"Prepared {len(df)} mapping records")
        return df

    @staticmethod
    def load_subject_data(filepath: str) -> pd.DataFrame:
        """
        Load subject summary data from CSV file

        Args:
            filepath: Path to the subject summary CSV file

        Returns:
            Cleaned subject DataFrame
        """
        logger.info(f"Loading subject data from {filepath}")

        try:
            df = pd.read_csv(filepath)
            logger.info(f"Loaded {len(df)} subject records from file")
            return DataLoader.prepare_subject_data(df)

        except Exception as e:
            logger.error(f"Error loading subject data: {e}")
            raise

    @staticmethod
    def load_mapping_data(filepath: str) -> pd.DataFrame:
        """
        Load treatment mapping data from CSV file

        Args:
            filepath: Path to the treatment mapping CSV file

        Returns:
            Cleaned mapping DataFrame
        """
        logger.info(f"Loading treatment mapping data from {filepath}")

        try:
            df = pd.read_csv(filepath)
            logger.info(f"Loaded {len(df)} mapping records from file")
            return DataLoader.prepare_mapping_data(df)

        except Exception as e:
            logger.error(f"Error loading mapping data: {e}")
            raise


# ============================================================================
# TEXT PROCESSING UTILITIES
# ============================================================================

class TextProcessor:
    """Utilities for text normalization and parsing"""

    @staticmethod
    def normalize_text(text: Any) -> str:
        """
        Normalize text for consistent matching

        Args:
            text: Input text to normalize

        Returns:
            Normalized lowercase text with cleaned quotes
        """
        if pd.isna(text):
            return str(text)

        text = str(text)
        # Replace smart quotes with regular quotes
        text = text.replace("'", "'").replace(""", '"').replace(""", '"')
        # Strip whitespace and convert to lowercase
        return text.strip().lower()

    @staticmethod
    def parse_cycle_day(visit_string: str) -> int:
        """
        Parse the day number from visit strings like 'TPC C20D1' or 'Cycle 46 Day 8'

        Args:
            visit_string: String describing the visit

        Returns:
            Day number (defaults to 1 if unparsable)
        """
        if pd.isna(visit_string):
            return 1

        visit_str = str(visit_string)

        # Try to match patterns like D1, Day 1, Day1
        match = re.search(r'(?:D|Day\s?)(\d+)', visit_str, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return 1

        # If it's a cycle string without a day, assume day 1
        if 'cycle' in visit_str.lower():
            return 1

        return 1

    @staticmethod
    def parse_cycle_number(visit_string: str) -> int:
        """
        Parse the cycle number from visit strings like 'TPC C20D1' or 'Cycle 46 Day 8'

        Args:
            visit_string: String describing the visit

        Returns:
            Cycle number (defaults to 0 if unparsable)
        """
        if pd.isna(visit_string):
            return 0

        visit_str = str(visit_string)

        # Try to match patterns like C20, Cycle 46, Cycle46
        match = re.search(r'(?:C|Cycle\s?)(\d+)', visit_str, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return 0

        return 0

    @staticmethod
    def parse_visit_days(visit_days_str: str) -> List[int]:
        """
        Parse visit days from a comma-separated string

        Args:
            visit_days_str: String like "1,8,15" or "1, 8, 15"

        Returns:
            List of day numbers
        """
        if pd.isna(visit_days_str) or str(visit_days_str).lower() == 'nan':
            return []

        try:
            days = str(visit_days_str).split(',')
            return [int(day.strip()) for day in days if day.strip().isdigit()]
        except Exception as e:
            logger.warning(f"Could not parse visit days '{visit_days_str}': {e}")
            return []


# ============================================================================
# VISIT PROJECTION ENGINE
# ============================================================================

class VisitProjector:
    """Handles the projection of future visits for patients"""

    def __init__(self, text_processor: TextProcessor):
        self.text_processor = text_processor

    def project_future_visits(self, row: pd.Series) -> List[ProjectedVisit]:
        """
        Project future visits for a patient

        This implementation matches the Databricks logic:
        1. Projects remaining visits in the current cycle
        2. Projects future complete cycles
        3. Rolls forward cycles if they're in the past

        Args:
            row: Patient data row with merged treatment information

        Returns:
            List of ProjectedVisit objects
        """
        projected_visits = []

        try:
            # Get last visit date and validate
            last_visit_date = pd.to_datetime(row['last_study_visit_date'])
            if pd.isna(last_visit_date):
                return []

            # Get dispensing frequency and validate
            dispensing_frequency = row.get('dispensing_frequency_days', 28)
            if pd.isna(dispensing_frequency):
                dispensing_frequency = 28
            cycle_days = int(dispensing_frequency)

            # Get visit days pattern
            visit_days_str = row.get('visit_days', '')
            visit_days = self.text_processor.parse_visit_days(visit_days_str)
            if not visit_days:
                visit_days = [1]  # Default to day 1 only
            visit_days = sorted(visit_days)

            # Get cycle information
            last_day_number = row.get('parsed_last_visit_day', 1)
            current_cycle_number = row.get('parsed_last_visit_cycle', 0)
            is_crossover = row.get('Is Crossover', False)
            is_tpc = row.get('Is TPC', False)

            # Get max cycles (optional constraint - hard cap on cycle numbers)
            max_cycles = row.get('max_cycles', None)
            if not pd.isna(max_cycles) and max_cycles >= 1:
                max_cycles = int(max_cycles)
            else:
                max_cycles = None  # No cycle limit, only time limit applies

            # Calculate Day 1 of the last recorded cycle (current cycle)
            time_to_subtract = timedelta(days=last_day_number - 1)
            last_cycle_day_1 = last_visit_date - time_to_subtract

            # Determine prefix for forecast string
            if is_crossover:
                prefix = "Crossover "
            elif is_tpc:
                prefix = "TPC "
            else:
                prefix = ""

            # Get today's date and projection horizon for time-based filtering
            TODAY = datetime.now().date()
            projection_horizon = TODAY + timedelta(days=Config.DEFAULT_PROJECTION_DAYS)

            # ============================================================================
            # A. PROJECT REMAINING VISITS IN CURRENT CYCLE
            # ============================================================================
            remaining_days = [day for day in visit_days if day > last_day_number]

            for day in remaining_days:
                visit_date = last_cycle_day_1 + timedelta(days=day - 1)

                # Only include if the projected date is:
                # 1. After the last recorded visit
                # 2. Within the projection horizon (next 365 days)
                # 3. Within max_cycles if defined
                if visit_date.date() > last_visit_date.date() and visit_date.date() <= projection_horizon:
                    # Check max_cycles constraint if defined
                    if max_cycles is not None and current_cycle_number > max_cycles:
                        continue

                    recorded_forecast_str = f"{prefix}Cycle {current_cycle_number} Day {day}"

                    projected_visit = ProjectedVisit(
                        subject_number=int(row['subject_number']),
                        medicine_name=row['medicine_name'],
                        cycle_number=current_cycle_number,
                        cycle_day=day,
                        visit_date=visit_date.strftime('%Y-%m-%d'),
                        medicine_quantity=row['total_medicines_required_per_cycle'],
                        visit_description=recorded_forecast_str
                    )

                    projected_visits.append(projected_visit)

            # ============================================================================
            # B. PROJECT NEXT FULL CYCLES (TIME-BASED WITH MAX_CYCLES CAP)
            # ============================================================================

            # Calculate Day 1 of the NEXT cycle
            cycle_duration = timedelta(days=cycle_days)
            forecast_start_day_1 = last_cycle_day_1 + cycle_duration

            # Roll forward if the calculated start is in the past
            if forecast_start_day_1.date() < TODAY:
                days_since_start = (TODAY - forecast_start_day_1.date()).days
                missed_cycles = days_since_start // cycle_days + 1
                cycle_day_1 = forecast_start_day_1 + missed_cycles * cycle_duration
            else:
                cycle_day_1 = forecast_start_day_1

            # Determine the cycle number to start the future projection from
            start_cycle_number = current_cycle_number + 1

            # Project cycles until we exceed the time horizon
            # Loop stops when visit dates exceed projection_horizon OR max_cycles is reached
            cycle_offset = 0
            while True:
                # Calculate Day 1 of the current future forecast cycle
                current_cycle_day_1 = cycle_day_1 + timedelta(days=cycle_offset * cycle_days)
                current_projected_cycle = start_cycle_number + cycle_offset

                # Check if we've exceeded max cycles (hard cap if defined)
                if max_cycles is not None and current_projected_cycle > max_cycles:
                    break

                # Check if the first day of this cycle exceeds our time horizon
                # If so, we still need to check individual visit days in case some are within horizon
                any_visit_in_horizon = False

                for day in visit_days:
                    visit_date = current_cycle_day_1 + timedelta(days=day - 1)

                    # Only include visits within the projection horizon
                    if visit_date.date() <= projection_horizon:
                        any_visit_in_horizon = True

                        recorded_forecast_str = f"{prefix}Cycle {current_projected_cycle} Day {day}"

                        projected_visit = ProjectedVisit(
                            subject_number=int(row['subject_number']),
                            medicine_name=row['medicine_name'],
                            cycle_number=current_projected_cycle,
                            cycle_day=day,
                            visit_date=visit_date.strftime('%Y-%m-%d'),
                            medicine_quantity=row['total_medicines_required_per_cycle'],
                            visit_description=recorded_forecast_str
                        )

                        projected_visits.append(projected_visit)

                # If no visits in this cycle were within the horizon, we're done
                if not any_visit_in_horizon:
                    break

                cycle_offset += 1

        except Exception as e:
            logger.error(f"Error projecting visits for subject {row.get('subject_number', 'unknown')}: {e}")

        return projected_visits


# ============================================================================
# MAIN DEMAND PLANNING PROCESSOR
# ============================================================================

class DemandPlanningProcessor:
    """Main processor that orchestrates the demand planning workflow"""

    def __init__(self):
        self.data_loader = DataLoader()
        self.text_processor = TextProcessor()
        self.visit_projector = VisitProjector(self.text_processor)

    def filter_active_subjects(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter to only include active subjects

        Args:
            df: Subject DataFrame

        Returns:
            Filtered DataFrame with only active subjects
        """
        logger.info("Filtering to active subjects only")

        initial_count = len(df)
        df_filtered = df[~df["subject_status"].isin(Config.EXCLUDED_STATUSES)]
        final_count = len(df_filtered)

        logger.info(f"Filtered from {initial_count} to {final_count} active subjects")
        return df_filtered

    def normalize_data(self, df_subjects: pd.DataFrame, df_mapping: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Normalize text fields in both DataFrames for consistent matching

        Args:
            df_subjects: Subject DataFrame
            df_mapping: Treatment mapping DataFrame

        Returns:
            Tuple of normalized DataFrames
        """
        logger.info("Normalizing data for consistent matching")

        # Define columns to normalize
        normalize_columns = ["study_protocol", "randomized_treatment", "tpc", "country"]

        # Normalize subject data
        for col in normalize_columns:
            if col in df_subjects.columns:
                df_subjects[col] = df_subjects[col].apply(self.text_processor.normalize_text)

        # Normalize mapping data
        for col in normalize_columns:
            if col in df_mapping.columns:
                df_mapping[col] = df_mapping[col].apply(self.text_processor.normalize_text)

        # Also normalize drug columns in mapping
        drug_columns = ["study_drug_dispensed", "additional_study_drug_dispensed"]
        for col in drug_columns:
            if col in df_mapping.columns:
                df_mapping[col] = df_mapping[col].apply(self.text_processor.normalize_text)

        return df_subjects, df_mapping

    def merge_and_calculate(self, df_subjects: pd.DataFrame, df_mapping: pd.DataFrame) -> pd.DataFrame:
        """
        Merge subject and mapping data, then calculate medicine requirements

        Args:
            df_subjects: Subject DataFrame
            df_mapping: Treatment mapping DataFrame

        Returns:
            Merged DataFrame with calculated medicine requirements
        """
        logger.info("Merging subject and treatment mapping data")

        # Define merge keys
        merge_keys = ["study_protocol", "randomized_treatment", "tpc", "subject_status"]

        # Perform inner merge
        df_merged = pd.merge(df_subjects, df_mapping, on=merge_keys, how="inner")
        logger.info(f"Merged resulted in {len(df_merged)} records")

        # Calculate visit count per cycle
        def count_visits(visit_days_str):
            visits = self.text_processor.parse_visit_days(visit_days_str)
            return len(visits) if visits else 0

        df_merged["visit_count_per_cycle"] = df_merged["visit_days"].apply(count_visits)

        # Calculate total medicines required per cycle
        df_merged["total_medicines_required_per_cycle"] = (
                df_merged["visit_count_per_cycle"] * df_merged["dispensing_quantity"]
        )

        # Identify the specific medicine for each row
        df_merged["medicine_name"] = np.where(
            df_merged["study_drug_dispensed"] != "nan",
            df_merged["study_drug_dispensed"],
            df_merged["additional_study_drug_dispensed"]
        )

        # Filter out rows without a medicine
        df_result = df_merged[df_merged["medicine_name"] != "nan"].copy()
        logger.info(f"Identified {len(df_result)} records with valid medicines")

        # Add parsed visit information for easier processing
        df_result['parsed_last_visit_cycle'] = df_result['last_study_visit_recorded'].apply(
            self.text_processor.parse_cycle_number
        )
        df_result['parsed_last_visit_day'] = df_result['last_study_visit_recorded'].apply(
            self.text_processor.parse_cycle_day
        )

        return df_result

    def aggregate_by_patient_medicine(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate data by patient and medicine combination

        Args:
            df: DataFrame with calculated medicine requirements

        Returns:
            Aggregated DataFrame with one row per patient-medicine combination
        """
        logger.info("Aggregating by patient and medicine")

        # Define aggregation
        id_cols = ["subject_number", "medicine_name"]

        # Columns to sum
        sum_cols = ["total_medicines_required_per_cycle"]

        # All other columns - take the first value
        other_cols = [col for col in df.columns if col not in id_cols + sum_cols]

        agg_dict = {col: 'first' for col in other_cols}
        agg_dict.update({col: 'sum' for col in sum_cols})

        df_aggregated = df.groupby(id_cols, dropna=False).agg(agg_dict).reset_index()

        logger.info(f"Aggregated to {len(df_aggregated)} patient-medicine combinations")

        # Add flags for Crossover and TPC to help with prefix generation
        df_aggregated['Is Crossover'] = df_aggregated['last_study_visit_recorded'].astype(str).str.contains('Crossover', case=False)
        df_aggregated['Is TPC'] = df_aggregated['last_study_visit_recorded'].astype(str).str.contains('tpc', case=False)

        return df_aggregated

    def project_all_visits(self, df_plan: pd.DataFrame) -> pd.DataFrame:
        """
        Project future visits for all patients

        Args:
            df_plan: Aggregated patient-medicine DataFrame

        Returns:
            DataFrame with all projected visits
        """
        logger.info("Projecting future visits for all patients")

        all_visits = []
        error_subjects = []

        total_rows = len(df_plan)

        for idx, row in df_plan.iterrows():
            if idx % 100 == 0:
                logger.info(f"Processing row {idx}/{total_rows}")

            try:
                visits = self.visit_projector.project_future_visits(row)

                # Convert ProjectedVisit objects to dictionaries
                for visit in visits:
                    all_visits.append({
                        'subject_number': visit.subject_number,
                        'medicine_name': visit.medicine_name,
                        'cycle': visit.cycle_number,
                        'day': visit.cycle_day,
                        'predicted_next_visit_date': visit.visit_date,
                        'total_medicine_required_forecast': visit.medicine_quantity,
                        'predicted_study_visit': visit.visit_description
                    })

            except Exception as e:
                error_subjects.append(row['subject_number'])
                logger.error(f"Error projecting visits for subject {row['subject_number']}: {e}")

        if error_subjects:
            logger.warning(f"Encountered errors for {len(set(error_subjects))} subjects")

        df_visits = pd.DataFrame(all_visits)
        logger.info(f"Generated {len(df_visits)} projected visits")

        return df_visits

    def prepare_final_output(self, df_visits: pd.DataFrame, df_plan: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the final output by merging projections with patient details

        Args:
            df_visits: DataFrame with projected visits
            df_plan: Original patient-medicine plan DataFrame

        Returns:
            Final formatted DataFrame ready for output
        """
        logger.info("Preparing final output")

        # Define columns to keep from the plan
        plan_columns = [
            'subject_number', 'site_id', 'parent_depot', 'subject_status',
            'randomized_treatment', 'tpc', 'medicine_name', 'country',
            'study_protocol', 'visit_days', 'visit_count_per_cycle',
            'dispensing_quantity', 'dispensing_frequency_days', 'date_randomized',
            'last_study_visit_recorded', 'last_study_visit_date',
            'last_study_visit_number', 'total_medicines_required_per_cycle',
            'study_drug_dispensed', 'additional_study_drug_dispensed',
            'parsed_last_visit_cycle', 'parsed_last_visit_day', 'processed_timestamp'
        ]

        # Keep only columns that exist
        plan_columns = [col for col in plan_columns if col in df_plan.columns]

        # Merge projections with plan details
        df_final = pd.merge(
            df_visits,
            df_plan[plan_columns],
            on=['subject_number', 'medicine_name'],
            how='left'
        )

        # Rename columns for final output
        df_final.rename(columns={
            'study_protocol': 'study_name',
            'medicine_name': 'drug_dispensed',
            'country': 'subject_country'
        }, inplace=True)

        # Define final column order
        final_columns = [
            'study_name', 'parent_depot', 'site_id', 'subject_number',
            'subject_status', 'subject_country', 'randomized_treatment', 'tpc', 'drug_dispensed',
            'dispensing_quantity', 'predicted_study_visit', 'cycle', 'day',
            'predicted_next_visit_date', 'processed_timestamp'
        ]

        # Keep only columns that exist
        final_columns = [col for col in final_columns if col in df_final.columns]
        df_final = df_final[final_columns]

        # Sort for better readability
        df_final = df_final.sort_values(
            by=['study_name', 'parent_depot', 'site_id', 'subject_number', 'cycle', 'day']
        )

        logger.info(f"Final output contains {len(df_final)} records")
        return df_final

    def run(self,
            subject_file: Optional[str] = None,
            mapping_file: Optional[str] = None,
            output_file: Optional[str] = None,
            df_subjects: Optional[pd.DataFrame] = None,
            df_mapping: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Run the complete demand planning process

        Args:
            subject_file: Path to subject summary CSV (used if df_subjects not provided)
            mapping_file: Path to treatment mapping CSV (used if df_mapping not provided)
            output_file: Path for output CSV (optional, if None will not save to file)
            df_subjects: Subject DataFrame (if provided, subject_file is ignored)
            df_mapping: Treatment mapping DataFrame (if provided, mapping_file is ignored)

        Returns:
            Final demand forecast DataFrame
        """
        logger.info("=" * 60)
        logger.info("Starting Demand Planning Process")
        logger.info("=" * 60)

        # Load data - either from dataframes or from files
        if df_subjects is None:
            if subject_file is None:
                raise ValueError("Either df_subjects or subject_file must be provided")
            logger.info(f"Loading subject data from file: {subject_file}")
            df_subjects = self.data_loader.load_subject_data(subject_file)
        else:
            logger.info(f"Using provided subject DataFrame with {len(df_subjects)} records")
            # Make a copy to avoid modifying the original
            df_subjects = df_subjects.copy()
            # Apply the same preparation logic that load_subject_data uses
            df_subjects = self.data_loader.prepare_subject_data(df_subjects)

        if df_mapping is None:
            if mapping_file is None:
                raise ValueError("Either df_mapping or mapping_file must be provided")
            logger.info(f"Loading mapping data from file: {mapping_file}")
            df_mapping = self.data_loader.load_mapping_data(mapping_file)
        else:
            logger.info(f"Using provided mapping DataFrame with {len(df_mapping)} records")
            # Make a copy to avoid modifying the original
            df_mapping = df_mapping.copy()
            # Apply the same preparation logic that load_mapping_data uses
            df_mapping = self.data_loader.prepare_mapping_data(df_mapping)

        # Filter to active subjects
        df_subjects = self.filter_active_subjects(df_subjects)

        # Normalize data
        df_subjects, df_mapping = self.normalize_data(df_subjects, df_mapping)

        # Merge and calculate requirements
        df_merged = self.merge_and_calculate(df_subjects, df_mapping)

        # Handle country column after merge (may become country_x and country_y)
        if 'country_x' in df_merged.columns:
            df_merged.rename(columns={'country_x': 'country'}, inplace=True)
        if 'country_y' in df_merged.columns and 'country' not in df_merged.columns:
            df_merged.rename(columns={'country_y': 'country'}, inplace=True)

        # Aggregate by patient-medicine
        df_plan = self.aggregate_by_patient_medicine(df_merged)



        # Project future visits
        df_visits = self.project_all_visits(df_plan)

        # Prepare final output
        df_final = self.prepare_final_output(df_visits, df_plan)

        # Save to file (optional)
        if output_file is not None:
            logger.info(f"Saving results to {output_file}")
            df_final.to_csv(output_file, index=False)
        else:
            logger.info("Output file not specified, skipping file save")

        logger.info("=" * 60)
        logger.info("Demand Planning Process Complete")
        logger.info("=" * 60)

        return df_final


# ============================================================================
# CONVENIENCE FUNCTIONS FOR NOTEBOOK USAGE
# ============================================================================

def run_demand_planning(df_subjects: pd.DataFrame,
                        df_mapping: pd.DataFrame,
                        output_file: Optional[str] = None) -> pd.DataFrame:
    """
    Convenience function for running demand planning from a notebook with dataframes

    Usage in Databricks notebook:
    ```python
    from demand_planning import run_demand_planning

    # Read your data (from Delta tables, CSV, etc.)
    df_subjects = spark.table("your_subject_table").toPandas()
    df_mapping = spark.table("your_mapping_table").toPandas()

    # Run demand planning
    df_forecast = run_demand_planning(df_subjects, df_mapping)

    # Optionally save to CSV
    df_forecast = run_demand_planning(df_subjects, df_mapping, output_file="output.csv")
    ```

    Args:
        df_subjects: Subject summary DataFrame
        df_mapping: Treatment mapping DataFrame
        output_file: Optional path to save output CSV

    Returns:
        Final demand forecast DataFrame
    """
    processor = DemandPlanningProcessor()
    return processor.run(df_subjects=df_subjects, df_mapping=df_mapping, output_file=output_file)


# ============================================================================
# MAIN EXECUTION (FOR LOCAL DEBUGGING)
# ============================================================================

def main():
    """Main execution function for local debugging with CSV files"""

    # Initialize the processor
    processor = DemandPlanningProcessor()

    # Define file paths
    subject_file = Config.SUBJECT_SUMMARY_FILE
    mapping_file = Config.TREATMENT_MAPPING_FILE
    output_file = Config.OUTPUT_FILE

    try:
        # Run the process with file paths
        result_df = processor.run(subject_file=subject_file,
                                  mapping_file=mapping_file,
                                  output_file=output_file)

        # Display summary statistics
        print("\n" + "=" * 60)
        print("SUMMARY STATISTICS")
        print("=" * 60)
        print(f"Total projected visits: {len(result_df)}")
        print(f"Unique patients: {result_df['subject_number'].nunique()}")
        print(f"Unique drugs: {result_df['drug_dispensed'].nunique()}")
        print(
            f"Date range: {result_df['predicted_next_visit_date'].min()} to {result_df['predicted_next_visit_date'].max()}")

        # Display first few rows
        print("\n" + "=" * 60)
        print("SAMPLE OUTPUT (First 10 rows)")
        print("=" * 60)
        print(result_df.head(10).to_string())

    except Exception as e:
        logger.error(f"Failed to complete demand planning: {e}")
        raise


if __name__ == "__main__":
    main()