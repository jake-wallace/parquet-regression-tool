import pandas as pd
import pyarrow.parquet as pq
import click
from pathlib import Path
from dataclasses import dataclass

from .inference import infer_sort_keys, infer_datetime_columns
from .comparison import compare_dataframes
from .reporting import ReportGenerator
from .checksum import generate_checksum


@dataclass
class ComparisonResult:
    status: str
    details: str = ""
    report_path: Path = None


class ParquetComparator:
    def __init__(
        self,
        file_before: Path,
        file_after: Path,
        output_dir: Path,
        config: dict,
        rules: dict,
    ):
        self.file_before = file_before
        self.file_after = file_after
        self.output_dir = output_dir
        self.config = config
        self.rules = rules

    def _check_schemas(self):
        schema_before = pq.read_schema(self.file_before)
        schema_after = pq.read_schema(self.file_after)
        if not schema_before.equals(schema_after):
            return f"Schema mismatch.\n\nBefore:\n{schema_before}\n\nAfter:\n{schema_after}"
        return None

    def _run_checksum_comparison(self):
        click.echo("  -> Stage 1: Attempting fast checksum comparison...")
        try:
            hash_before, keys_before = generate_checksum(
                self.file_before, self.config, self.rules
            )
            hash_after, keys_after = generate_checksum(
                self.file_after, self.config, self.rules
            )

            if not hash_before or not hash_after:
                click.echo("  -> Checksum failed: Could not determine a sort key.")
                return "CHECKSUM_FAILED", None

            if keys_before != keys_after:
                click.echo(
                    f"  -> Checksum failed: Inferred sort keys do not match ('{keys_before}' vs '{keys_after}')."
                )
                return "CHECKSUM_FAILED", None

            click.echo(f"  -> Using inferred sort key(s) for checksum: {keys_before}")
            if hash_before == hash_after:
                return "CHECKSUM_MATCH", keys_before
            else:
                return "CHECKSUM_MISMATCH", keys_before
        except Exception as e:
            click.secho(f"  -> Checksum error: {e}", fg="red")
            return "CHECKSUM_FAILED", None

    def run(self, skip_checksum: bool = False) -> ComparisonResult:
        # Always check schema first
        schema_diff = self._check_schemas()
        if schema_diff:
            # If schema differs, generate a report and exit. This is a critical failure.
            report_generator = ReportGenerator(
                self.file_before,
                self.file_after,
                self.output_dir,
                results=None,
                inferred_keys=[],
                schema_diff=schema_diff,
            )
            report_path = report_generator.generate_html_report()
            report_generator.print_summary()
            return ComparisonResult(
                status="SCHEMA_MISMATCH", details=schema_diff, report_path=report_path
            )

        # Initialize variables
        checksum_status = None
        inferred_keys = []

        if not skip_checksum:
            checksum_status, inferred_keys = self._run_checksum_comparison()
            if checksum_status == "CHECKSUM_MATCH":
                return ComparisonResult(status="IDENTICAL (CHECKSUM_MATCH)")
            if checksum_status == "CHECKSUM_MISMATCH":
                click.echo("  -> Checksums differ. Proceeding to detailed comparison.")

        click.echo("  -> Stage 2: Performing detailed row-by-row comparison...")
        try:
            df_before = pd.read_parquet(self.file_before)
            df_after = pd.read_parquet(self.file_after)
        except Exception as e:
            return ComparisonResult(status="READ_ERROR", details=str(e))

        if self.rules["ignore_columns"]:
            df_before = df_before.drop(
                columns=[
                    c for c in self.rules["ignore_columns"] if c in df_before.columns
                ],
                errors="ignore",
            )
            df_after = df_after.drop(
                columns=[
                    c for c in self.rules["ignore_columns"] if c in df_after.columns
                ],
                errors="ignore",
            )

        # Use keys from checksum if available, otherwise infer again
        sort_keys = inferred_keys or infer_sort_keys(
            df_before, self.config["key_uniqueness_threshold"]
        )
        if not sort_keys:
            return ComparisonResult(
                status="NO_SORT_KEY",
                details="Could not infer a unique key for detailed comparison.",
            )

        if not inferred_keys:  # Only print if we are inferring for the first time
            click.echo(f"  -> Using inferred sort key(s) for diff: {sort_keys}")

        datetime_cols = infer_datetime_columns(
            df_before, 1000, self.config["datetime_parse_threshold"]
        )
        for col in datetime_cols:
            if col in df_before.columns:
                df_before[col] = pd.to_datetime(df_before[col], errors="coerce")
            if col in df_after.columns:
                df_after[col] = pd.to_datetime(df_after[col], errors="coerce")

        comparison_results = compare_dataframes(
            df_before, df_after, sort_keys, self.rules["float_tolerance"]
        )

        report_generator = ReportGenerator(
            self.file_before,
            self.file_after,
            self.output_dir,
            comparison_results,
            sort_keys,
            schema_diff,
        )
        report_generator.print_summary()
        report_path = report_generator.generate_html_report()

        # Determine the final status
        if comparison_results.is_identical:
            # If checksum failed but detailed diff passed, it's a tolerance match
            status = (
                "IDENTICAL (TOLERANCE_MATCH)"
                if checksum_status == "CHECKSUM_MISMATCH"
                else "IDENTICAL"
            )
        else:
            status = "DIFFERENCES_FOUND"

        return ComparisonResult(status=status, report_path=report_path)
