import pandas as pd
import numpy as np
from pathlib import Path
import shutil


def generate_data():
    """
    Generates a comprehensive suite of dummy Parquet files for testing
    the parquet-comparator tool.
    """
    # Clean up previous data
    if Path("data").exists():
        shutil.rmtree("data")
        print("Removed existing data directory.")

    # Create base directories
    path_before = Path("data/before")
    path_after = Path("data/after")
    path_before.mkdir(parents=True, exist_ok=True)
    path_after.mkdir(parents=True, exist_ok=True)
    print("Created new data directories.")

    # --- Test Case 1: Identical but Reordered Data (Checksum test) ---
    df_before = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "product_name": ["Apple", "Banana", "Cherry", "Date"],
            "stock": [100, 150, 200, 50],
        }
    )
    df_after = df_before.sample(frac=1).reset_index(drop=True)  # Shuffle the dataframe
    df_before.to_parquet(path_before / "01_identical_but_reordered.parquet")
    df_after.to_parquet(path_after / "01_identical_but_reordered.parquet")
    print("Generated: 01_identical_but_reordered.parquet")

    # --- Test Case 2: Float Tolerance Change (Should be IDENTICAL) ---
    df_before = pd.DataFrame(
        {
            "sensor_id": ["A-1", "B-2", "C-3"],
            "reading": [1.2345678, 2.3456789, 3.4567890],
        }
    )
    df_after = df_before.copy()
    df_after.loc[1, "reading"] += 1e-7  # Change smaller than default 1e-6 tolerance
    df_before.to_parquet(path_before / "02_float_tolerance_change.parquet")
    df_after.to_parquet(path_after / "02_float_tolerance_change.parquet")
    print("Generated: 02_float_tolerance_change.parquet")

    # --- Test Case 3: Significant Float Change (Should be a MODIFICATION) ---
    df_before = pd.DataFrame(
        {"sensor_id": ["A-1", "B-2", "C-3"], "reading": [1.2345, 2.3456, 3.4567]}
    )
    df_after = df_before.copy()
    df_after.loc[1, "reading"] += 1e-4  # Change larger than default tolerance
    df_before.to_parquet(path_before / "03_float_significant_change.parquet")
    df_after.to_parquet(path_after / "03_float_significant_change.parquet")
    print("Generated: 03_float_significant_change.parquet")

    # --- Test Case 4: String Value Change ---
    df_before = pd.DataFrame(
        {"user_id": [101, 102, 103], "status": ["active", "inactive", "active"]}
    )
    df_after = df_before.copy()
    df_after.loc[1, "status"] = "suspended"
    df_before.to_parquet(path_before / "04_string_value_change.parquet")
    df_after.to_parquet(path_after / "04_string_value_change.parquet")
    print("Generated: 04_string_value_change.parquet")

    # --- Test Case 5: Datetime Value Change ---
    df_before = pd.DataFrame(
        {
            "event_id": ["evt01", "evt02", "evt03"],
            "timestamp": pd.to_datetime(
                ["2023-10-27 10:00:00", "2023-10-27 11:00:00", "2023-10-27 12:00:00"]
            ),
        }
    )
    df_after = df_before.copy()
    df_after.loc[0, "timestamp"] = pd.to_datetime("2023-10-27 10:05:00")
    df_before.to_parquet(path_before / "05_datetime_value_change.parquet")
    df_after.to_parquet(path_after / "05_datetime_value_change.parquet")
    print("Generated: 05_datetime_value_change.parquet")

    # --- Test Case 6: Rows Added and Deleted ---
    df_base = pd.DataFrame(
        {
            "item_sku": ["SKU-001", "SKU-002", "SKU-003", "SKU-004"],
            "price": [19.99, 29.99, 9.99, 49.99],
        }
    )
    df_before = df_base[
        df_base["item_sku"].isin(["SKU-001", "SKU-002", "SKU-003"])
    ]  # Keep 1, 2, 3
    df_after = df_base[
        df_base["item_sku"].isin(["SKU-001", "SKU-003", "SKU-004"])
    ]  # Keep 1, 3, 4
    df_before.to_parquet(path_before / "06_rows_added_and_deleted.parquet")
    df_after.to_parquet(path_after / "06_rows_added_and_deleted.parquet")
    print("Generated: 06_rows_added_and_deleted.parquet")

    # --- Test Case 7: All Changes Combined ---
    df_before = pd.DataFrame(
        {
            "log_id": [10, 20, 30, 40],
            "level": ["INFO", "WARN", "INFO", "ERROR"],
            "message": [
                "Process started",
                "Disk space low",
                "Data loaded",
                "Connection failed",
            ],
        }
    )
    df_after = pd.DataFrame(
        {
            "log_id": [50, 20, 10, 30],  # 40 deleted, 50 added, order changed
            "level": ["INFO", "CRITICAL", "INFO", "DEBUG"],  # 20 and 30 changed
            "message": [
                "New process started",
                "Disk space critical",
                "Process started",
                "Data loaded successfully",
            ],
        }
    )
    df_before.to_parquet(path_before / "07_all_changes_combined.parquet")
    df_after.to_parquet(path_after / "07_all_changes_combined.parquet")
    print("Generated: 07_all_changes_combined.parquet")

    # --- Test Case 8: Schema Change (Column Added) ---
    df_before = pd.DataFrame({"id": [1, 2], "data": ["a", "b"]})
    df_after = pd.DataFrame(
        {"id": [1, 2], "data": ["a", "b"], "new_col": [True, False]}
    )
    df_before.to_parquet(path_before / "08_schema_col_added.parquet")
    df_after.to_parquet(path_after / "08_schema_col_added.parquet")
    print("Generated: 08_schema_col_added.parquet")

    # --- Test Case 9: Schema Change (Column Type Changed) ---
    df_before = pd.DataFrame({"id": [1, 2], "value": [100, 200]})  # value is int
    df_after = pd.DataFrame({"id": [1, 2], "value": ["100", "200"]})  # value is string
    df_before.to_parquet(path_before / "09_schema_col_type_changed.parquet")
    df_after.to_parquet(path_after / "09_schema_col_type_changed.parquet")
    print("Generated: 09_schema_col_type_changed.parquet")

    # --- Test Case 10: No Obvious Key (Inference should fail) ---
    df_before = pd.DataFrame(
        {"city": ["A", "A", "B", "B", "C"], "value": [10, 20, 10, 30, 40]}
    )
    df_after = df_before.copy()
    df_after.loc[0, "value"] = 15
    df_before.to_parquet(path_before / "10_no_obvious_key.parquet")
    df_after.to_parquet(path_after / "10_no_obvious_key.parquet")
    print("Generated: 10_no_obvious_key.parquet")

    # --- Test Case 11: Fails Only With a Stricter, File-Specific Tolerance ---
    df_before = pd.DataFrame(
        {"device_id": ["dev-001", "dev-002"], "voltage": [3.30251, 4.98765]}
    )
    df_after = df_before.copy()
    # This change (0.000005) is smaller than the global tolerance (0.00001)
    # but larger than a stricter tolerance we will define in the config (0.0000001).
    df_after.loc[0, "voltage"] += 5e-6
    df_before.to_parquet(path_before / "11_strict_tolerance_fail.parquet")
    df_after.to_parquet(path_after / "11_strict_tolerance_fail.parquet")
    print("Generated: 11_strict_tolerance_fail.parquet")

    # --- Test Case 12: Fuzzy Matching with "Evil Twin" Rows ---
    # This tests if the weighted logic can distinguish between two very
    # similar rows and correctly identify which one was modified.
    df_before = pd.DataFrame({
        'customer_id': ['CUST-ABC', 'CUST-ABC', 'CUST-XYZ'],
        'product_name': ['3-Port USB Hub', '3-Port USB Hubb', 'Wireless Mouse'], # Note the typo in the second row
        'status': ['SHIPPED', 'SHIPPED', 'DELIVERED']
    })
    
    # The 'after' file changes the status of the row that HAD the typo.
    # The weighted logic should use the high-cardinality 'product_name'
    # as an anchor to correctly pair the rows and find the modification.
    df_after = pd.DataFrame({
        'customer_id': ['CUST-XYZ', 'CUST-ABC', 'CUST-ABC'], # Reordered
        'product_name': ['Wireless Mouse', '3-Port USB Hub', '3-Port USB Hubb'],
        'status': ['DELIVERED', 'SHIPPED', 'RETURNED'] # This status changed
    })
    
    df_before.to_parquet(path_before / "12_fuzzy_evil_twin.parquet")
    df_after.to_parquet(path_after / "12_fuzzy_evil_twin.parquet")
    print("Generated: 12_fuzzy_evil_twin.parquet")

    print("\nDummy data generation complete.")


if __name__ == "__main__":
    generate_data()
