# Parquet Regression Comparison Tool

![Status](https://img.shields.io/badge/Status-Active-brightgreen)

A powerful and configurable Python CLI tool for performing regression testing on Parquet files. It intelligently compares two directories of Parquet files ("before" and "after" a code change), identifies differences, and generates detailed console summaries and visual HTML reports.

This tool is designed to be generic and scalable, requiring no prior knowledge of the file schemas. It automatically infers unique keys for comparison, handles row-order changes, and provides tolerance-based comparisons for floating-point data.

---

## Key Features

-   **Automated File Discovery**: Recursively scans and pairs matching Parquet files in two directory trees.
-   **Intelligent Key Inference**: Automatically identifies the most likely unique key(s) in a file to enable robust comparison even when row order changes.
-   **Order-Independent Checksum**: Uses a fast, content-based checksum to quickly identify identical files and skip unnecessary detailed comparisons.
-   **Categorized Diffing**: Clearly categorizes all data differences into **Added**, **Deleted**, and **Modified** rows.
-   **Schema Validation**: Detects and reports schema mismatches (e.g., added/removed columns, data type changes) before attempting a data-level comparison.
-   **Configurable Float Tolerance**: Avoids test failures from insignificant floating-point precision changes by allowing a configurable tolerance.
-   **Rich Reporting**:
    -   Detailed summary printed to the console for quick feedback.
    -   Generates a clean, visual **HTML report** for every file with differences, pinpointing the exact changes.
-   **Stateful Tracking**: Keeps a lightweight SQLite database to track processed files, avoiding redundant work on subsequent runs.
-   **Generic and Configurable**: Works out-of-the-box with any Parquet files and allows global behavior to be customized via a simple `config.yaml` file.

---

## Setup and Installation

### Prerequisites

-   Python 3.9+
-   Git

### 1. Clone the Repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd parquet-regression-tool```

### 2. Set Up a Virtual Environment

It is highly recommended to use a virtual environment to manage dependencies.

```bash
# Create the environment
python -m venv venv

# Activate it (on Mac/Linux)
source venv/bin/activate

# Or, activate it (on Windows)
.\venv\Scripts\activate
```

### 3. Install Dependencies

Install all required Python libraries using the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

---

## How to Use

### 1. Place Your Parquet Files

Place the "before" and "after" versions of your data into two separate directories. The tool expects the folder structure within these two directories to be mirrored.

```
.
├── data/
│   ├── before/
│   │   ├── transactions.parquet
│   │   └── nested_folder/
│   │       └── user_profiles.parquet
│   └── after/
│       ├── transactions.parquet
│       └── nested_folder/
│           └── user_profiles.parquet
...
```

### 2. Configure the Comparison

Edit the `config.yaml` file to define the behavior of the tool.

```yaml
# Point to your data directories
base_path_before: "./data/before"
base_path_after: "./data/after"

# Specify where to save reports and the tracking database
output_directory: "./reports"

# Set a global tolerance for comparing floating-point numbers
float_tolerance: 1.0e-6

# (Optional) Provide a list of column names to ignore in all comparisons
global_ignore_columns:
  - "etl_load_timestamp"
  - "last_updated_by"

# --- File Specific Overrides (Optional) ---
# Apply stricter or different rules to files matching a certain pattern
file_specific_rules:
  - pattern: "**/critical_data.parquet"
    # For this file, use a much stricter tolerance
    float_tolerance: 1e-9
```

### 3. Run the Comparator

Execute the main script from the root of the project directory.

```bash
python run_comparator.py
```

#### Command-Line Options

-   `--config-file` or `-c`: Specify a different path to the configuration file. (Default: `config.yaml`)
-   `--force` or `-f`: Force a re-comparison of all files, ignoring the tracking log of previously processed files.
-   `--no-checksum`: Skip the fast checksum stage and proceed directly to a detailed, row-by-row comparison for all files.

### 4. Interpret the Results

#### Console Output

The tool provides real-time feedback in your terminal, showing the status for each file pair:

-   `IDENTICAL (CHECKSUM_MATCH)`: The files are identical. The fast path succeeded.
-   `IDENTICAL (TOLERANCE_MATCH)`: The files are not bit-for-bit identical, but all differences were within the configured `float_tolerance`. A report is generated for auditing.
-   `DIFFERENCES_FOUND`: The files have meaningful data differences. A report is generated.
-   `SCHEMA_MISMATCH`: The file schemas are different. A report is generated detailing the schema changes.
-   `NO_SORT_KEY`: The tool could not confidently infer a unique key for comparison.

#### Visual HTML Reports

For any file that is not a perfect checksum match, a detailed HTML report will be generated in the `output_directory` (e.g., `./reports/`).

Open these `.html` files in any web browser to see:
-   A high-level summary of the comparison.
-   A detailed breakdown of schema differences (if any).
-   Clean tables showing every row that was **Added**, **Deleted**, or **Modified**, with changed values highlighted.

This allows for quick and easy visual analysis of any regression in your data.