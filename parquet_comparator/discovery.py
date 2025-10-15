from pathlib import Path
from typing import Iterator, Tuple


def pair_files(base_before: Path, base_after: Path) -> Iterator[Tuple[Path, Path]]:
    """
    Finds all .parquet files in the 'before' directory and yields pairs
    with their corresponding files in the 'after' directory.
    """
    if not base_before.is_dir() or not base_after.is_dir():
        raise FileNotFoundError("One of the base directories does not exist.")

    for file_before in base_before.rglob("*.parquet"):
        relative_path = file_before.relative_to(base_before)
        file_after = base_after / relative_path

        if file_after.exists():
            yield file_before, file_after
