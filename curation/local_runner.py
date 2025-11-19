"""
Local Runner for Data Curation - Simplified for PyCharm

Simple script for local debugging. Just edit the configuration section below
and run this file in PyCharm (no command line arguments needed).

Steps:
1. Download CSV files from Databricks/S3 to local directory
2. Download mapping Excel file
3. Edit the CONFIGURATION section below
4. Run this file (Right-click → Run in PyCharm)
5. Check the output directory for results
"""

import os
from pathlib import Path
from typing import List
import pandas as pd

from data_curator import DataCurator, load_excel_mapping, logger


# ============================================================================
# CONFIGURATION - Edit these paths for your local debugging
# ============================================================================

# Directory containing your CSV files
DATA_DIR = "test_data/20251106"

# Date folder (e.g., "20251106")
DATE_FOLDER = "20251106"

# Path to mapping Excel file
MAPPING_FILE = "test_data/mapping/Subject Summary Header Mapping.xlsx"

# Output directory for processed files
OUTPUT_DIR = "./output"

# Which file types to process (comment out lines you don't want)
PROCESS_FILE_TYPES = [
    'subject',
    'depot',
    'site',
    'slevel_supplymethod',
    'clevel_supplymethod',
]

# ============================================================================
# Column Mappings (same as Databricks notebook)
# ============================================================================

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
            "date_randomized", "date_treatment_discontinued", "date_crossover_enrolled", "date_crossover_approved",
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
    },

    "slevel_supplymethod": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Country": "country",
            "Site ID": "site_id",
            "Comparator Name": "comparator_name",
            "Site Level Supply Method": "site_level_supply_method",
            "Site Status": "site_status",
        },
        "date_columns": []
    },

    "clevel_supplymethod": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Country": "country",
            "Comparator Name": "comparator_name",
            "Country Level Supply Method": "country_level_supply_method"
        },
        "date_columns": []
    }
}


# ============================================================================
# Helper Functions
# ============================================================================

def find_csv_files_by_type(data_dir: Path, file_type: str) -> List[str]:
    """Find CSV files of a specific type in the data directory."""
    files = []

    logger.info(f"Scanning for {file_type} files in: {data_dir}")

    for file_path in data_dir.rglob('*.csv'):
        file_name_lower = file_path.name.lower()

        # Categorize files
        if file_type == 'subject' and 'subject summary' in file_name_lower:
            files.append(str(file_path))
        elif file_type == 'depot' and 'depot' in file_name_lower and 'inventory' in file_name_lower:
            files.append(str(file_path))
        elif file_type == 'site' and 'site' in file_name_lower and 'inventory' in file_name_lower:
            files.append(str(file_path))
        elif file_type == 'slevel_supplymethod' and 'site' in file_name_lower and 'supplymethod' in file_name_lower:
            files.append(str(file_path))
        elif file_type == 'clevel_supplymethod' and 'country' in file_name_lower and 'supplymethod' in file_name_lower:
            files.append(str(file_path))

    logger.info(f"Found {len(files)} {file_type} files")
    return files


# ============================================================================
# Main Processing
# ============================================================================

def main():
    """Main processing function."""

    logger.info("=" * 80)
    logger.info("Starting Local Data Curation")
    logger.info("=" * 80)
    logger.info(f"Data Directory: {DATA_DIR}")
    logger.info(f"Date Folder: {DATE_FOLDER}")
    logger.info(f"Mapping File: {MAPPING_FILE}")
    logger.info(f"Output Directory: {OUTPUT_DIR}")
    logger.info(f"Processing file types: {PROCESS_FILE_TYPES}")

    # Validate inputs
    data_dir = Path(DATA_DIR)
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return

    mapping_path = Path(MAPPING_FILE)
    if not mapping_path.exists():
        logger.error(f"Mapping file not found: {mapping_path}")
        return

    # Create output directory
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load mapping and initialize curator
    logger.info("\n→ Loading mapping file...")
    mapping_df = load_excel_mapping(str(mapping_path))
    curator = DataCurator(mapping_df=mapping_df)
    logger.info("✓ DataCurator initialized")

    # Process each file type
    results = {}

    if 'subject' in PROCESS_FILE_TYPES:
        logger.info("\n" + "=" * 80)
        logger.info("Processing Subject Summary files")
        logger.info("=" * 80)

        subject_files = find_csv_files_by_type(data_dir, 'subject')

        if subject_files:
            subject_result = curator.process_subject_summary_batch_from_files(
                file_paths=subject_files,
                date_folder=DATE_FOLDER,
                column_mapping=MAPPING_CONFIG['subject']['column_mapping'],
                date_columns=MAPPING_CONFIG['subject']['date_columns']
            )

            if subject_result is not None:
                output_path = output_dir / f'subject_summary_{DATE_FOLDER}.csv'
                subject_result.to_csv(output_path, index=False)
                logger.info(f"✓ Saved Subject Summary: {output_path}")
                logger.info(f"  Shape: {subject_result.shape}")
                results['subject'] = subject_result

    if 'depot' in PROCESS_FILE_TYPES:
        logger.info("\n" + "=" * 80)
        logger.info("Processing Depot Inventory files")
        logger.info("=" * 80)

        depot_files = find_csv_files_by_type(data_dir, 'depot')

        if depot_files:
            depot_result = curator.process_generic_batch_from_files(
                file_paths=depot_files,
                date_folder=DATE_FOLDER,
                file_type='depot',
                column_mapping=MAPPING_CONFIG['depot']['column_mapping'],
                date_columns=MAPPING_CONFIG['depot']['date_columns']
            )

            if depot_result is not None:
                output_path = output_dir / f'depot_inventory_{DATE_FOLDER}.csv'
                depot_result.to_csv(output_path, index=False)
                logger.info(f"✓ Saved Depot Inventory: {output_path}")
                logger.info(f"  Shape: {depot_result.shape}")
                results['depot'] = depot_result

    if 'site' in PROCESS_FILE_TYPES:
        logger.info("\n" + "=" * 80)
        logger.info("Processing Site Inventory files")
        logger.info("=" * 80)

        site_files = find_csv_files_by_type(data_dir, 'site')

        if site_files:
            site_result = curator.process_generic_batch_from_files(
                file_paths=site_files,
                date_folder=DATE_FOLDER,
                file_type='site',
                column_mapping=MAPPING_CONFIG['site']['column_mapping'],
                date_columns=MAPPING_CONFIG['site']['date_columns']
            )

            if site_result is not None:
                output_path = output_dir / f'site_inventory_{DATE_FOLDER}.csv'
                site_result.to_csv(output_path, index=False)
                logger.info(f"✓ Saved Site Inventory: {output_path}")
                logger.info(f"  Shape: {site_result.shape}")
                results['site'] = site_result

    if 'slevel_supplymethod' in PROCESS_FILE_TYPES:
        logger.info("\n" + "=" * 80)
        logger.info("Processing Site-Level Supply Method files")
        logger.info("=" * 80)

        slevel_files = find_csv_files_by_type(data_dir, 'slevel_supplymethod')

        if slevel_files:
            slevel_result = curator.process_generic_batch_from_files(
                file_paths=slevel_files,
                date_folder=DATE_FOLDER,
                file_type='site-level supply method',
                column_mapping=MAPPING_CONFIG['slevel_supplymethod']['column_mapping'],
                date_columns=MAPPING_CONFIG['slevel_supplymethod']['date_columns']
            )

            if slevel_result is not None:
                output_path = output_dir / f'slevel_supplymethod_{DATE_FOLDER}.csv'
                slevel_result.to_csv(output_path, index=False)
                logger.info(f"✓ Saved Site-Level Supply Method: {output_path}")
                logger.info(f"  Shape: {slevel_result.shape}")
                results['slevel_supplymethod'] = slevel_result

    if 'clevel_supplymethod' in PROCESS_FILE_TYPES:
        logger.info("\n" + "=" * 80)
        logger.info("Processing Country-Level Supply Method files")
        logger.info("=" * 80)

        clevel_files = find_csv_files_by_type(data_dir, 'clevel_supplymethod')

        if clevel_files:
            clevel_result = curator.process_generic_batch_from_files(
                file_paths=clevel_files,
                date_folder=DATE_FOLDER,
                file_type='country-level supply method',
                column_mapping=MAPPING_CONFIG['clevel_supplymethod']['column_mapping'],
                date_columns=MAPPING_CONFIG['clevel_supplymethod']['date_columns']
            )

            if clevel_result is not None:
                output_path = output_dir / f'clevel_supplymethod_{DATE_FOLDER}.csv'
                clevel_result.to_csv(output_path, index=False)
                logger.info(f"✓ Saved Country-Level Supply Method: {output_path}")
                logger.info(f"  Shape: {clevel_result.shape}")
                results['clevel_supplymethod'] = clevel_result

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
