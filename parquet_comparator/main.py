import polars as pl
import pyarrow.parquet as pq
from pathlib import Path
from dataclasses import dataclass

from .inference import infer_sort_keys_pl
from .comparison import compare_dataframes_pl, ComparisonData
from .reporting import ReportGenerator
from .checksum import generate_checksum_pl
from .fuzzy_comparison_pandas import fuzzy_compare_dataframes as fuzzy_compare_pandas


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

    def run(self, skip_checksum: bool = False) -> ComparisonResult:
        schema_diff = self._check_schemas()
        if schema_diff:
            report_generator = ReportGenerator(
                self.file_before,
                self.file_after,
                self.output_dir,
                results=None,
                inferred_keys=[],
                schema_diff=schema_diff,
            )
            report_path = report_generator.generate_html_report()
            return ComparisonResult(
                status="SCHEMA_MISMATCH", details=schema_diff, report_path=report_path
            )

        checksum_status, inferred_keys = None, []
        if not skip_checksum:
            checksum_status, inferred_keys = generate_checksum_pl(
                self.file_before, self.config, self.rules
            )
            if checksum_status == "CHECKSUM_MATCH":
                return ComparisonResult(status="IDENTICAL (CHECKSUM_MATCH)")
            if checksum_status == "CHECKSUM_MISMATCH":
                pass  # Continue to detailed diff

        try:
            df_before = pl.read_parquet(self.file_before)
            df_after = pl.read_parquet(self.file_after)
        except Exception as e:
            return ComparisonResult(status="READ_ERROR", details=str(e))

        if self.rules["ignore_columns"]:
            # Polars requires checking if columns exist before dropping
            cols_to_drop_before = [
                c for c in self.rules["ignore_columns"] if c in df_before.columns
            ]
            cols_to_drop_after = [
                c for c in self.rules["ignore_columns"] if c in df_after.columns
            ]
            df_before = df_before.drop(cols_to_drop_before)
            df_after = df_after.drop(cols_to_drop_after)

        sort_keys = inferred_keys or infer_sort_keys_pl(
            df_before, self.config["key_uniqueness_threshold"]
        )

        inferred_keys_for_report = []
        status = ""

        if not sort_keys:
            fuzzy_threshold = self.config.get("fuzzy_match_threshold", 0.8)

            # Bridge to pandas for the fuzzy comparison
            pd_before = df_before.to_pandas()
            pd_after = df_after.to_pandas()
            pd_results = fuzzy_compare_pandas(pd_before, pd_after, fuzzy_threshold)

            # Convert results back to Polars for consistent reporting
            comparison_results = ComparisonData(
                added=pl.from_pandas(pd_results.added),
                deleted=pl.from_pandas(pd_results.deleted),
                modified=pl.from_pandas(pd_results.modified.reset_index()),
                is_identical=pd_results.is_identical,
            )

            status = (
                "FUZZY_IDENTICAL"
                if comparison_results.is_identical
                else "FUZZY_DIFFERENCES_FOUND"
            )
            inferred_keys_for_report = ["(Fuzzy Match)"]
        else:
            comparison_results = compare_dataframes_pl(
                df_before, df_after, sort_keys, self.rules["float_tolerance"]
            )

            if comparison_results.is_identical:
                status = (
                    "IDENTICAL (TOLERANCE_MATCH)"
                    if checksum_status == "CHECKSUM_MISMATCH"
                    else "IDENTICAL"
                )
            else:
                status = "DIFFERENCES_FOUND"
            inferred_keys_for_report = sort_keys

        report_generator = ReportGenerator(
            self.file_before,
            self.file_after,
            self.output_dir,
            results=comparison_results,
            inferred_keys=inferred_keys_for_report,
            schema_diff=schema_diff,
        )
        report_path = report_generator.generate_html_report()

        return ComparisonResult(status=status, report_path=report_path)
