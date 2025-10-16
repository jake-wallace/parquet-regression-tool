import polars as pl
from pathlib import Path
from typing import Tuple, List, Optional

from .inference import infer_sort_keys_pl


def generate_checksum_pl(
    file_path: Path, config: dict, rules: dict
) -> Tuple[Optional[str], Optional[List[str]]]:
    df = pl.read_parquet(file_path)

    if rules["ignore_columns"]:
        df = df.drop(rules["ignore_columns"])

    sort_keys = infer_sort_keys_pl(df, config.get("key_uniqueness_threshold", 0.99))
    if not sort_keys:
        return None, None

    df_sorted = df.sort(by=sort_keys)

    checksum = df_sorted.hash_rows(seed=42).to_list()[0]

    return str(checksum), sort_keys
