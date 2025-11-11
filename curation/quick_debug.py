"""
Quick Local Debugging Script

This script demonstrates how to use DataCurator directly with file paths
for quick local debugging without needing the full local_runner.py script.

Usage:
    python curation/quick_debug.py
"""

from data_curator import DataCurator, load_excel_mapping

# Configuration - same as Databricks notebook
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


def main():
    """Quick debugging example."""

    print("=" * 80)
    print("DataCurator Local Debugging Example")
    print("=" * 80)

    # ========================================================================
    # STEP 1: Configure your paths
    # ========================================================================

    # Path to your mapping Excel file
    MAPPING_FILE = "./test_data/header_mapping.xlsx"

    # Paths to your CSV files (from Databricks/S3)
    SUBJECT_FILES = [
        "./test_data/GS-US-592-6173_Subject_Summary.csv",
        "./test_data/GS-US-592-6789_Subject_Summary.csv"
    ]

    DEPOT_FILES = [
        "./test_data/GS-US-592-6173_Depot_Inventory.csv"
    ]

    SITE_FILES = [
        "./test_data/GS-US-592-6173_Site_Inventory.csv"
    ]

    DATE_FOLDER = "20251106"

    # ========================================================================
    # STEP 2: Load mapping and initialize curator
    # ========================================================================

    print("\n→ Loading mapping file...")
    mapping_df = load_excel_mapping(MAPPING_FILE)
    print(f"✓ Loaded mapping: {len(mapping_df)} rows")

    print("\n→ Initializing DataCurator...")
    curator = DataCurator(mapping_df=mapping_df)
    print("✓ DataCurator initialized")

    # ========================================================================
    # STEP 3: Process Subject Summary files (just pass file paths!)
    # ========================================================================

    print("\n" + "=" * 80)
    print("Processing Subject Summary Files")
    print("=" * 80)

    subject_result = curator.process_subject_summary_batch_from_files(
        file_paths=SUBJECT_FILES,
        date_folder=DATE_FOLDER,
        column_mapping=MAPPING_CONFIG["subject"]["column_mapping"],
        date_columns=MAPPING_CONFIG["subject"]["date_columns"]
    )

    if subject_result is not None:
        print(f"\n✓ Processed Subject Summary: {subject_result.shape}")
        print(f"  Columns: {list(subject_result.columns[:5])}...")
        print(f"\n  First few rows:")
        print(subject_result.head())

        # Save to CSV for inspection
        output_file = "./output/subject_summary_debug.csv"
        subject_result.to_csv(output_file, index=False)
        print(f"\n✓ Saved to: {output_file}")

    # ========================================================================
    # STEP 4: Process Depot files
    # ========================================================================

    print("\n" + "=" * 80)
    print("Processing Depot Files")
    print("=" * 80)

    depot_result = curator.process_generic_batch_from_files(
        file_paths=DEPOT_FILES,
        date_folder=DATE_FOLDER,
        file_type="depot",
        column_mapping=MAPPING_CONFIG["depot"]["column_mapping"],
        date_columns=MAPPING_CONFIG["depot"]["date_columns"]
    )

    if depot_result is not None:
        print(f"\n✓ Processed Depot Inventory: {depot_result.shape}")
        print(f"  Columns: {list(depot_result.columns[:5])}...")
        print(f"\n  First few rows:")
        print(depot_result.head())

        # Save to CSV for inspection
        output_file = "./output/depot_inventory_debug.csv"
        depot_result.to_csv(output_file, index=False)
        print(f"\n✓ Saved to: {output_file}")

    # ========================================================================
    # STEP 5: Process Site files
    # ========================================================================

    print("\n" + "=" * 80)
    print("Processing Site Files")
    print("=" * 80)

    site_result = curator.process_generic_batch_from_files(
        file_paths=SITE_FILES,
        date_folder=DATE_FOLDER,
        file_type="site",
        column_mapping=MAPPING_CONFIG["site"]["column_mapping"],
        date_columns=MAPPING_CONFIG["site"]["date_columns"]
    )

    if site_result is not None:
        print(f"\n✓ Processed Site Inventory: {site_result.shape}")
        print(f"  Columns: {list(site_result.columns[:5])}...")
        print(f"\n  First few rows:")
        print(site_result.head())

        # Save to CSV for inspection
        output_file = "./output/site_inventory_debug.csv"
        site_result.to_csv(output_file, index=False)
        print(f"\n✓ Saved to: {output_file}")

    print("\n" + "=" * 80)
    print("Processing Complete!")
    print("=" * 80)
    print("\nTip: If you hit an error, use Python debugger:")
    print("  import pdb; pdb.set_trace()")
    print("Or use your IDE's debugger to step through the code")


if __name__ == '__main__':
    main()
