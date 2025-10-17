import polars as pl
from typing import Tuple, List, Optional


def generate_checksum_pl(
    df: pl.DataFrame, sort_keys: list
) -> Tuple[Optional[str], Optional[List[str]]]:
    """
    Generates an order-independent checksum for a Polars DataFrame.

    Args:
        df: The pre-loaded DataFrame to hash.
        sort_keys: The list of keys to sort by before hashing.

    Returns:
        A tuple containing the checksum hash and the keys used.
    """
    if not sort_keys or not all(key in df.columns for key in sort_keys):
        return None, None

    try:
        df_sorted = df.sort(by=sort_keys)
        checksum = df_sorted.hash_rows(seed=42)[0]
        return str(checksum), sort_keys
    except pl.ColumnNotFoundError:
        return None, None
