import pandas as pd
import hashlib
from pathlib import Path
from typing import Tuple, List, Optional

from .inference import infer_sort_keys


def generate_checksum(
    file_path: Path, config: dict, rules: dict
) -> Tuple[Optional[str], Optional[List[str]]]:
    """
    Generates an order-independent checksum for a Parquet file.
    This version relies on inferring the sort key.
    """
    df = pd.read_parquet(file_path)

    # Apply global ignore rules before hashing
    if rules["ignore_columns"]:
        cols_to_drop = [c for c in rules["ignore_columns"] if c in df.columns]
        df = df.drop(columns=cols_to_drop)

    # Always infer sort keys
    sort_keys = infer_sort_keys(df, config.get("key_uniqueness_threshold", 0.99))

    if not sort_keys:
        return None, None  # Cannot create a reliable checksum without a key

    # Create the canonical representation
    df.sort_values(by=sort_keys, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Generate hash from the sorted dataframe
    hasher = hashlib.sha256()
    hash_series = pd.util.hash_pandas_object(df, index=False)
    hasher.update(hash_series.values)

    return hasher.hexdigest(), sort_keys
