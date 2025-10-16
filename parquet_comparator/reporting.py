import polars as pl
import pandas as pd
from pathlib import Path
import datetime
from jinja2 import Environment, FileSystemLoader


class ReportGenerator:
    def __init__(
        self, file_before, file_after, output_dir, results, inferred_keys, schema_diff
    ):
        self.file_before = file_before
        self.file_after = file_after
        self.output_dir = output_dir
        self.results = results
        self.inferred_keys = inferred_keys
        self.schema_diff = schema_diff
        self.summary = self._create_summary()

    def _create_summary(self):
        summary = {
            "filename": self.file_before.name,
            "file_before": str(
                self.file_before
            ),  # Use string representation for the report
            "file_after": str(
                self.file_after
            ),  # Use string representation for the report
            "inferred_keys": self.inferred_keys,
            "timestamp": datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),  # MOVED timestamp here
        }

        if self.schema_diff:
            summary["status"] = "SCHEMA_MISMATCH"
            summary.update(
                {
                    "rows_before": "N/A",
                    "rows_after": "N/A",
                    "rows_added": "N/A",
                    "rows_deleted": "N/A",
                    "rows_modified": "N/A",
                }
            )
            return summary

        if self.results:
            rows_added = self.results.added.height
            rows_deleted = self.results.deleted.height

            if self.results.modified.height > 0:
                # Polars' n_unique() on a column named 'key' will work here
                rows_modified = self.results.modified.select(pl.col("key")).n_unique()
            else:
                rows_modified = 0

            # Estimate row counts based on diffs
            rows_in_common = rows_modified
            rows_before = rows_in_common + rows_deleted
            rows_after = rows_in_common + rows_added

            is_fuzzy = self.inferred_keys and self.inferred_keys[0] == "(Fuzzy Match)"
            if self.results.is_identical:
                summary["status"] = "FUZZY_IDENTICAL" if is_fuzzy else "IDENTICAL"
            else:
                summary["status"] = (
                    "FUZZY_DIFFERENCES_FOUND" if is_fuzzy else "DIFFERENCES_FOUND"
                )

            summary.update(
                {
                    "rows_before": rows_before,
                    "rows_after": rows_after,
                    "rows_added": rows_added,
                    "rows_deleted": rows_deleted,
                    "rows_modified": rows_modified,
                }
            )

        return summary

    def print_summary(self):
        # This function is used by the main process for console output, so it's fine.
        pass

    def generate_html_report(self) -> Path:
        template_dir = Path(__file__).parent.parent / "templates"
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template("report_template.html")

        def to_html(df: pl.DataFrame):
            if df is None or df.height == 0:
                return None
            pd_df = df.to_pandas()
            if "key" in pd_df.columns:
                pd_df = pd_df.set_index("key")
            return pd_df.to_html()

        # The data passed to the template is now cleaner and more consistent.
        report_data = {
            "summary": self.summary,
            "schema_diff": self.schema_diff,
            "modified_rows_html": to_html(
                self.results.modified if self.results else None
            ),
            "added_rows_html": to_html(self.results.added if self.results else None),
            "deleted_rows_html": to_html(
                self.results.deleted if self.results else None
            ),
        }

        html_content = template.render(report_data)

        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"report_{self.file_before.stem}_{ts}.html"
        report_path = self.output_dir / report_filename

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        return report_path
