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
            "file_before": self.file_before,
            "file_after": self.file_after,
            "inferred_keys": self.inferred_keys,  
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

            def count_unique_rows(df):
                if df.empty:
                    return 0
                return len(df.index.unique())

            rows_in_common = count_unique_rows(self.results.modified)
            rows_deleted = len(self.results.deleted)
            rows_added = len(self.results.added)

            is_fuzzy = (
                self.inferred_keys and self.inferred_keys[0] == "(Fuzzy Match)"
            )  # CORRECTED
            if self.results.is_identical:
                summary["status"] = "FUZZY_IDENTICAL" if is_fuzzy else "IDENTICAL"
            else:
                summary["status"] = (
                    "FUZZY_DIFFERENCES_FOUND" if is_fuzzy else "DIFFERENCES_FOUND"
                )

            summary.update(
                {
                    "rows_before": rows_in_common + rows_deleted,
                    "rows_after": rows_in_common + rows_added,
                    "rows_added": rows_added,
                    "rows_deleted": rows_deleted,
                    "rows_modified": count_unique_rows(self.results.modified),
                }
            )

        return summary

    def print_summary(self):
        print("  --- Comparison Summary ---")
        if self.schema_diff:
            print(f"  Schema Status: MISMATCH")
        elif self.results:
            print(f"  Schema Status: IDENTICAL")
            print(f"  Rows Added:    {self.summary['rows_added']}")
            print(f"  Rows Deleted:  {self.summary['rows_deleted']}")
            print(f"  Rows Modified: {self.summary['rows_modified']}")
        print("  --------------------------")

    def generate_html_report(self) -> Path:
        template_dir = Path(__file__).parent.parent / "templates"
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template("report_template.html")

        report_data = {
            "file_before": str(self.file_before),
            "file_after": str(self.file_after),
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary": self.summary,
            "schema_diff": self.schema_diff,
            "modified_rows_html": (
                self.results.modified.to_html()
                if self.results and not self.results.modified.empty
                else None
            ),
            "added_rows_html": (
                self.results.added.to_html()
                if self.results and not self.results.added.empty
                else None
            ),
            "deleted_rows_html": (
                self.results.deleted.to_html()
                if self.results and not self.results.deleted.empty
                else None
            ),
        }

        html_content = template.render(report_data)

        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"report_{self.file_before.stem}_{ts}.html"
        report_path = self.output_dir / report_filename

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        return report_path
