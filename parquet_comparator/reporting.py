import polars as pl
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
            "file_before": str(self.file_before),
            "file_after": str(self.file_after),
            "inferred_keys": self.inferred_keys,
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "top_modified_fields": [],  # Default empty list
        }

        if not self.schema_diff.is_identical:
            summary["status"] = "SCHEMA_DIFFERENCES_FOUND"
        elif self.results and not self.results.is_identical:
            summary["status"] = "DATA_DIFFERENCES_FOUND"
        else:
            summary["status"] = "IDENTICAL"

        if self.results:
            rows_added = self.results.added.height
            rows_deleted = self.results.deleted.height
            rows_modified = (
                self.results.modified.select(pl.col("key")).n_unique()
                if self.results.modified.height > 0
                else 0
            )

            if self.results.modified.height > 0:
                top_fields = (
                    self.results.modified.group_by("column")
                    .agg(pl.count().alias("change_count"))
                    .sort("change_count", descending=True)
                    .head(5)
                ).to_dicts()
                summary["top_modified_fields"] = top_fields

            summary.update(
                {
                    "rows_before": "N/A",
                    "rows_after": "N/A",
                    "rows_added": rows_added,
                    "rows_deleted": rows_deleted,
                    "rows_modified": rows_modified,
                }
            )

        return summary

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
