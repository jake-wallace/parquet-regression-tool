import sqlite3
import datetime
from pathlib import Path


class ComparisonTracker:
    def __init__(self, db_path: Path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._create_table()

    def _create_table(self):
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS comparison_log (
                id INTEGER PRIMARY KEY,
                file_before TEXT NOT NULL,
                file_after TEXT NOT NULL,
                status TEXT NOT NULL,
                comparison_timestamp TEXT NOT NULL,
                report_path TEXT,
                UNIQUE(file_before, file_after)
            )
        """
        )
        self.conn.commit()

    def log_comparison(
        self, file_before: Path, file_after: Path, status: str, report_path: Path = None
    ):
        timestamp = datetime.datetime.utcnow().isoformat()
        report_str = str(report_path) if report_path else None

        self.cursor.execute(
            """
            INSERT OR REPLACE INTO comparison_log 
            (file_before, file_after, status, comparison_timestamp, report_path)
            VALUES (?, ?, ?, ?, ?)
        """,
            (str(file_before), str(file_after), status, timestamp, report_str),
        )
        self.conn.commit()

    def has_been_processed(self, file_before: Path, file_after: Path) -> bool:
        """Checks if a file pair has been successfully logged as IDENTICAL."""
        self.cursor.execute(
            "SELECT status FROM comparison_log WHERE file_before = ? AND file_after = ?",
            (str(file_before), str(file_after)),
        )
        result = self.cursor.fetchone()
        return result and "IDENTICAL" in result[0]

    def close(self):
        self.conn.close()
