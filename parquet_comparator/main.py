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
    """
    Compares two schemas and returns a structured SchemaDiff object
    detailing all differences found.
    """
    schema_before = pq.read_schema(file_before)
    schema_after = pq.read_schema(file_after)

    if schema_before.equals(schema_after):
        return SchemaDiff(is_identical=True)

    names_before = set(schema_before.names)
    names_after = set(schema_after.names)

    added = {
        name: str(schema_after.field(name).type) for name in names_after - names_before
    }
    removed = {
        name: str(schema_before.field(name).type) for name in names_before - names_after
    }

    type_changes = {}
    for name in names_before.intersection(names_after):
        field_before = schema_before.field(name)
        field_after = schema_after.field(name)
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
                "  -> Schema mismatch detected. Will proceed with data comparison.",
                fg="yellow",
            )

        checksum_status, inferred_keys = None, []
        if not skip_checksum and schema_diff.is_identical:
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
            click.secho(
                "  -> No unique key found. Falling back to fuzzy record linkage.",
                fg="yellow",
            )
            fuzzy_threshold = self.config.get("fuzzy_match_threshold", 0.8)
            pd_before = df_before.to_pandas()
            pd_after = df_after.to_pandas()
            pd_results = fuzzy_compare_pandas(pd_before, pd_after, fuzzy_threshold)

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
