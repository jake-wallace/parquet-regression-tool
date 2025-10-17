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
        # Safety check: if keys aren't in the dataframe, we can't sort.
        return None, None

    try:
        # Sort the DataFrame by the provided keys
        df_sorted = df.sort(by=sort_keys)

        # Polars' hash_rows is extremely fast and creates a single hash for the DF
        checksum = df_sorted.hash_rows(seed=42)[0]

        return str(checksum), sort_keys
    except pl.ColumnNotFoundError:
        # This catch is a fallback, but the check above should prevent it.
        return None, None
