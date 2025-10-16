# Parquet Comparison Tool

![Status](https://img.shields.io/badge/Status-Active-brightgreen) ![Python Version](https://img.shields.io/badge/Python-3.9+-blue) ![Powered by](https://img.shields.io/badge/Powered%20by-Polars%20%26%20PyArrow-purple)

A parallelized CLI tool for performing deep regression testing on Parquet files. It compares two directories of Parquet files ("before" and "after" a code change), identifies schema and data differences, and generates detailed console summaries and visual HTML reports.

Built for performance and large datasets, this tool leverages the **Polars** DataFrame library for lightning-fast, memory-efficient processing and uses **multiprocessing** to compare numerous files in parallel, fully utilizing modern multi-core CPUs.

---

## Core Philosophy & How It Works

This tool is designed to be a "zero-configuration" utility that adapts to your data, not the other way around. It uses a sophisticated **three-stage comparison process** to provide the most accurate and efficient diff possible.

### The Three-Stage Comparison Process

For each pair of matching files, the tool attempts the following stages in order:

#### Stage 1: Fast Checksum (Order-Independent)
The tool first attempts to prove the files are identical in the fastest way possible. It calculates an order-independent hash of each file's content by reading the data, sorting it by an automatically inferred unique key, and hashing the result.
-   **If hashes match (`IDENTICAL (CHECKSUM_MATCH)`)**: The files are functionally identical. The comparison stops here. No report is generated.
-   **If hashes differ**: The files are not identical. The tool proceeds to Stage 2.

#### Stage 2: Precise Keyed Comparison
If a reliable, unique key column (or set of columns) can be inferred from the data, the tool performs a highly accurate and efficient `join` operation.
-   This is the "happy path" for structured data.
-   It precisely categorizes every row-level difference as **Added**, **Deleted**, or **Modified**.
-   It can also identify functionally identical files where minor floating-point differences are within a configurable tolerance (`IDENTICAL (TOLERANCE_MATCH)`).

#### Stage 3: Fuzzy Record Linkage (Fallback)
If no unique key can be found, the tool does not give up. It falls back to a powerful probabilistic matching algorithm.
-   Instead of sorting, it "scores" the similarity between rows in the "before" and "after" files.
-   It uses **weighted column scoring** (giving more importance to columns with more unique data) to intelligently pair up rows that are "the same entity," even if they have been modified.
-   This allows it to produce an intuitive "Added/Deleted/Modified" report even for messy, keyless data, correctly identifying modifications instead of misclassifying them as a delete and an add.

---

## Key Features

-   **High-Performance Engine**: Built with **Polars** and **multiprocessing** to handle large files and large numbers of files with exceptional speed.
-   **Intelligent Key Inference**: Automatically detects unique keys to perform precise comparisons.
-   **Robust Fuzzy Matching**: Provides meaningful, row-level diffs even for datasets without a primary key.
-   **Flexible Schema Validation**:
    -   Detects and details schema differences (added/removed columns, type changes).
    -   **Does not hard-fail**. It proceeds to compare the data on common columns and even compares columns with changed types by casting them to strings.
-   **Rich, Auditable Reporting**:
    -   Real-time console output with a summary for each file.
    -   Generates a clean, visual **HTML report** for every file with differences, pinpointing the exact changes.
    -   Includes a "Top Modified Fields" summary in reports to highlight the most impacted columns.
-   **Stateful Tracking**: A lightweight SQLite database remembers which files have passed comparison, automatically skipping them on future runs to save time.
-   **Zero-Configuration Ready**: Works out-of-the-box with any Parquet files, with smart defaults that can be tuned via a simple `config.yaml`.

---

## Setup and Installation

### Prerequisites

-   Python 3.9+
-   Git

### 1. Clone the Repository & Navigate

```bash
git clone https://github.com/jake-wallace/parquet-regression-tool
cd parquet-regression-tool
```

### 2. Set Up a Virtual Environment

```bash
# Create the environment
python -m venv venv

# Activate it (on Mac/Linux)
source venv/bin/activate

# Or, activate it (on Windows)
.\venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## How to Use

### 1. Place Your Parquet Files

Place the "before" and "after" versions of your data into two separate directories. The tool matches files based on their **name and relative path**, so the folder structure must be mirrored.

```
.
├── data/
│   ├── before/
│   │   └── invoices/
│   │       └── daily-report-2023-11-01.parquet
│   └── after/
│       └── invoices/
│           └── daily-report-2023-11-01.parquet
...
```

### 2. Configure the Comparison (Optional)

Edit the `config.yaml` file to tune the tool's behavior. The default settings are designed to be a sensible starting point.

```yaml
# Point to your data directories
base_path_before: "./data/before"
base_path_after: "./data/after"

# Specify where to save reports and the tracking database
output_directory: "./reports"

# Global tolerance for float comparisons (default: 1.0e-6)
float_tolerance: 1.0e-6

# Minimum similarity score (0.0 to 1.0) for the fuzzy matching fallback
fuzzy_match_threshold: 0.85

# A list of column names to ignore in all comparisons
global_ignore_columns: ["etl_load_timestamp"]
```

### 3. Run the Comparator

Execute the main script from the root of the project directory.

```bash
# Run using all available CPU cores
python run_comparator.py

# Specify the number of parallel workers
python run_comparator.py --workers 4
```

#### Command-Line Options

Use `--help` to see all available options.

```bash
python run_comparator.py --help
```
-   `-f, --force`: Force a re-comparison of all files, ignoring the tracking log.
-   `-c, --config-file TEXT`: Specify a different path to the configuration file.
-   `-w, --workers INTEGER`: Set the number of parallel processes to use.

### 4. Interpret the Results

#### Console Output

The tool provides real-time feedback as each worker process completes its task. At the end of the run, you will see a summary of any **unmatched files** (files that existed in one directory but not the other).

#### Visual HTML Reports

For any file pair that has schema or data differences, a detailed HTML report is generated in the `output_directory` (e.g., `./reports/`).

Open these `.html` files in a web browser to see:
-   A prominent title with the filename.
-   A **Schema Differences** section detailing any added, removed, or type-changed columns.
-   A **Data Differences Summary** with counts for added/deleted/modified rows and a table of the **Top Modified Fields**.
-   Detailed tables showing every row that was **Added**, **Deleted**, or **Modified**. For modified rows, the `key` column will either be the unique key value or a `Fuzzy Match (Score: ...)` to indicate how the match was made.