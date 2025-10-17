import polars as pl
import pyarrow.parquet as pq
from pathlib import Path
from dataclasses import dataclass
import click

from .inference import infer_sort_keys_pl
from .comparison import compare_dataframes_pl, ComparisonData
from .reporting import ReportGenerator
from .checksum import generate_checksum_pl
from .fuzzy_comparison_pandas import fuzzy_compare_dataframes as fuzzy_compare_pandas
from .schemas import SchemaDiff


@dataclass
class ComparisonResult:
    status: str
    details: str = ""
    report_path: Path = None


def _check_schemas(file_before: Path, file_after: Path) -> SchemaDiff:
    """Compares two schemas and returns a structured SchemaDiff object."""
    schema_before = pq.read_schema(file_before)
    schema_after = pq.read_schema(file_after)

    if schema_before.equals(schema_after):
        return SchemaDiff(is_identical=True)

    names_before, names_after = set(schema_before.names), set(schema_after.names)
    added = {
        name: str(schema_after.field(name).type) for name in names_after - names_before
    }
    removed = {
        name: str(schema_before.field(name).type) for name in names_before - names_after
    }

    type_changes = {}
    for name in names_before.intersection(names_after):
        field_before, field_after = schema_before.field(name), schema_after.field(name)
        if not field_before.equals(field_after):
            type_changes[name] = (str(field_before.type), str(field_after.type))

    return SchemaDiff(
        is_identical=False,
        added_columns=added,
        removed_columns=removed,
        type_changes=type_changes,
    )


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

    def run(self, skip_checksum: bool = False) -> ComparisonResult:
        schema_diff = _check_schemas(self.file_before, self.file_after)
        if not schema_diff.is_identical:
            click.secho(
                "  -> Schema mismatch detected. Data comparison will proceed ONLY on common columns.",
                fg="yellow",
            )

        try:
            df_before = pl.read_parquet(self.file_before)
            df_after = pl.read_parquet(self.file_after)
        except Exception as e:
            return ComparisonResult(status="READ_ERROR", details=str(e))

        # Apply any user-defined ignore rules
        if self.rules["ignore_columns"]:
            df_before = df_before.drop(
                [c for c in self.rules["ignore_columns"] if c in df_before.columns]
            )
            df_after = df_after.drop(
                [c for c in self.rules["ignore_columns"] if c in df_after.columns]
            )

        common_cols = list(set(df_before.columns).intersection(set(df_after.columns)))
        df_before_common = df_before.select(common_cols)
        df_after_common = df_after.select(common_cols)

        sort_keys = infer_sort_keys_pl(
            df_before_common, self.config["key_uniqueness_threshold"]
        )

        checksum_status = None
        if not skip_checksum and sort_keys:
            # Checksum is run on common columns. We still report IDENTICAL if hashes match,
            # but the final report will show the schema difference.
            hash_before, _ = generate_checksum_pl(df_before_common, sort_keys)
            hash_after, _ = generate_checksum_pl(df_after_common, sort_keys)

            if hash_before and hash_after and hash_before == hash_after:
                # If content is identical but schemas differ, it's still a difference.
                if schema_diff.is_identical:
                    return ComparisonResult(status="IDENTICAL (CHECKSUM_MATCH)")
                else:
                    # Mark as a mismatch but proceed to generate a report showing only schema diffs
                    checksum_status = "CHECKSUM_MATCH_BUT_SCHEMA_DIFFERS"
            else:
                checksum_status = "CHECKSUM_MISMATCH"

        inferred_keys_for_report = []
        status = ""
        comparison_results = None

        if checksum_status == "CHECKSUM_MATCH_BUT_SCHEMA_DIFFERS":
            # Data is the same on common columns, but schema is different.
            # Create empty data diffs for the report.
            comparison_results = ComparisonData(is_identical=True)
            status = "DIFFERENCES_FOUND"  # Because schema is different
            inferred_keys_for_report = sort_keys
        elif not sort_keys:
            click.secho(
                "  -> No unique key found on common columns. Falling back to fuzzy record linkage.",
                fg="yellow",
            )
            fuzzy_threshold = self.config.get("fuzzy_match_threshold", 0.8)
            pd_before, pd_after = df_before.to_pandas(), df_after.to_pandas()
            pd_results = fuzzy_compare_pandas(pd_before, pd_after, fuzzy_threshold)

            comparison_results = ComparisonData(
                added=pl.from_pandas(pd_results.added),
                deleted=pl.from_pandas(pd_results.deleted),
                modified=pl.from_pandas(pd_results.modified.reset_index()),
                is_identical=pd_results.is_identical,
            )

            status = (
                "FUZZY_IDENTICAL"
                if comparison_results.is_identical and schema_diff.is_identical
                else "DIFFERENCES_FOUND"
            )
            inferred_keys_for_report = ["(Fuzzy Match)"]
        else:
            comparison_results = compare_dataframes_pl(
                df_before,
                df_after,
                sort_keys,
                self.rules["float_tolerance"],
                schema_diff,
            )

            if comparison_results.is_identical and schema_diff.is_identical:
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
