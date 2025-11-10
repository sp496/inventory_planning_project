"""
Local Runner for Data Curation

This script allows you to run the data curation process locally for debugging.
It reads CSV files from a local directory and processes them using DataCurator.

Usage:
    python local_runner.py --data-dir ./test_data --date-folder 20251106 --mapping ./mappings/header_mapping.xlsx

Requirements:
    - Input data directory with CSV files
    - Mapping Excel file
    - Treatment mapping Excel file (optional)
"""

import argparse
import os
from pathlib import Path
from typing import List, Tuple
import pandas as pd

from data_curator import (
    DataCurator,
    read_dynamic_csv,
    load_excel_mapping,
    load_treatment_mapping,
    logger
)


# Configuration (same as Databricks notebook)
MAPPING_CONFIG = {
    "subject": {
        "column_mapping": {
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
        "date_columns": [
            "date_randomized", "date_crossover_enrolled", "date_crossover_approved",
            "date_crossover_treatment_discontinued", "last_study_visit_date",
            "next_min_study_visit_date", "next_max_study_visit_date",
            "last_additional_drug_visit_date", "next_min_additional_drug_visit_date",
            "next_max_additional_drug_visit_date"
        ]
    },
    "depot": {
        "column_mapping": {
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
        "date_columns": ["fp_expiry_date"]
    },
    "site": {
        "column_mapping": {
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
        "date_columns": ["fp_expiry_date"]
    }
}


def find_csv_files(data_dir: Path) -> dict:
    """
    Find CSV files in the data directory and categorize them.

    Args:
        data_dir: Path to directory containing CSV files

    Returns:
        Dictionary with lists of (file_path, file_name) tuples by category
    """
    files = {
        'subject': [],
        'depot': [],
        'site': [],
        'slevel_supplymethod': [],
        'clevel_supplymethod': []
    }

    logger.info(f"Scanning directory: {data_dir}")

    for file_path in data_dir.rglob('*.csv'):
        file_name = file_path.name
        file_name_lower = file_name.lower()

        # Categorize files
        if 'subject summary' in file_name_lower:
            files['subject'].append((str(file_path), file_name))
            logger.info(f"Found Subject Summary: {file_name}")
        elif 'depot' in file_name_lower and 'inventory' in file_name_lower:
            files['depot'].append((str(file_path), file_name))
            logger.info(f"Found Depot Inventory: {file_name}")
        elif 'site' in file_name_lower and 'inventory' in file_name_lower:
            files['site'].append((str(file_path), file_name))
            logger.info(f"Found Site Inventory: {file_name}")
        elif 'site' in file_name_lower and 'supplymethod' in file_name_lower:
            files['slevel_supplymethod'].append((str(file_path), file_name))
            logger.info(f"Found Site Supply Method: {file_name}")
        elif 'country' in file_name_lower and 'supplymethod' in file_name_lower:
            files['clevel_supplymethod'].append((str(file_path), file_name))
            logger.info(f"Found Country Supply Method: {file_name}")

    return files


def load_dataframes(file_list: List[Tuple[str, str]]) -> List[Tuple[pd.DataFrame, str]]:
    """
    Load CSV files into DataFrames.

    Args:
        file_list: List of (file_path, file_name) tuples

    Returns:
        List of (DataFrame, filename) tuples
    """
    dataframes = []

    for file_path, file_name in file_list:
        try:
            df = read_dynamic_csv(file_path)
            dataframes.append((df, file_name))
            logger.info(f"Loaded {file_name}: {df.shape}")
        except Exception as e:
            logger.error(f"Error loading {file_name}: {str(e)}")
            continue

    return dataframes


def main():
    """Main function to run data curation locally."""
    parser = argparse.ArgumentParser(description='Run data curation locally for debugging')
    parser.add_argument('--data-dir', required=True, help='Directory containing CSV files')
    parser.add_argument('--date-folder', required=True, help='Date folder (e.g., 20251106)')
    parser.add_argument('--mapping', required=True, help='Path to header mapping Excel file')
    parser.add_argument('--output-dir', default='./output', help='Output directory for processed files')
    parser.add_argument('--file-type', choices=['subject', 'depot', 'site', 'all'],
                       default='all', help='Type of files to process')

    args = parser.parse_args()

    # Validate inputs
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return

    mapping_path = Path(args.mapping)
    if not mapping_path.exists():
        logger.error(f"Mapping file not found: {mapping_path}")
        return

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 80)
    logger.info("Starting Local Data Curation")
    logger.info("=" * 80)
    logger.info(f"Data Directory: {data_dir}")
    logger.info(f"Date Folder: {args.date_folder}")
    logger.info(f"Mapping File: {mapping_path}")
    logger.info(f"Output Directory: {output_dir}")

    # Load mapping
    logger.info("\nLoading mapping file...")
    mapping_df = load_excel_mapping(str(mapping_path))

    # Initialize curator
    curator = DataCurator(mapping_df=mapping_df)

    # Find all CSV files
    logger.info("\nScanning for CSV files...")
    csv_files = find_csv_files(data_dir)

    # Process each file type
    results = {}

    if args.file_type in ['subject', 'all'] and csv_files['subject']:
        logger.info("\n" + "=" * 80)
        logger.info("Processing Subject Summary files")
        logger.info("=" * 80)

        # Load dataframes
        subject_dfs = load_dataframes(csv_files['subject'])

        # Process
        subject_result = curator.process_subject_summary_batch(
            dataframes=subject_dfs,
            date_folder=args.date_folder,
            column_mapping=MAPPING_CONFIG['subject']['column_mapping'],
            date_columns=MAPPING_CONFIG['subject']['date_columns']
        )

        if subject_result is not None:
            output_path = output_dir / f'subject_summary_{args.date_folder}.csv'
            subject_result.to_csv(output_path, index=False)
            logger.info(f"✓ Saved Subject Summary: {output_path}")
            logger.info(f"  Shape: {subject_result.shape}")
            results['subject'] = subject_result

    if args.file_type in ['depot', 'all'] and csv_files['depot']:
        logger.info("\n" + "=" * 80)
        logger.info("Processing Depot Inventory files")
        logger.info("=" * 80)

        depot_dfs = load_dataframes(csv_files['depot'])

        depot_result = curator.process_generic_batch(
            dataframes=depot_dfs,
            date_folder=args.date_folder,
            file_type='depot',
            column_mapping=MAPPING_CONFIG['depot']['column_mapping'],
            date_columns=MAPPING_CONFIG['depot']['date_columns']
        )

        if depot_result is not None:
            output_path = output_dir / f'depot_inventory_{args.date_folder}.csv'
            depot_result.to_csv(output_path, index=False)
            logger.info(f"✓ Saved Depot Inventory: {output_path}")
            logger.info(f"  Shape: {depot_result.shape}")
            results['depot'] = depot_result

    if args.file_type in ['site', 'all'] and csv_files['site']:
        logger.info("\n" + "=" * 80)
        logger.info("Processing Site Inventory files")
        logger.info("=" * 80)

        site_dfs = load_dataframes(csv_files['site'])

        site_result = curator.process_generic_batch(
            dataframes=site_dfs,
            date_folder=args.date_folder,
            file_type='site',
            column_mapping=MAPPING_CONFIG['site']['column_mapping'],
            date_columns=MAPPING_CONFIG['site']['date_columns']
        )

        if site_result is not None:
            output_path = output_dir / f'site_inventory_{args.date_folder}.csv'
            site_result.to_csv(output_path, index=False)
            logger.info(f"✓ Saved Site Inventory: {output_path}")
            logger.info(f"  Shape: {site_result.shape}")
            results['site'] = site_result

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("Processing Complete")
    logger.info("=" * 80)
    logger.info(f"Processed {len(results)} file types")
    for file_type, df in results.items():
        logger.info(f"  {file_type}: {df.shape[0]} rows, {df.shape[1]} columns")
    logger.info(f"\nOutput files saved to: {output_dir}")


if __name__ == '__main__':
    main()
