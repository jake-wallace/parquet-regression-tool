import pandas as pd


def infer_sort_keys(df: pd.DataFrame, min_uniqueness_ratio: float = 0.99) -> list[str]:
    """Infers candidate sort keys from a DataFrame based on uniqueness."""
    candidate_keys = []
    num_rows = len(df)
    if num_rows == 0:
        return []

    for col in df.columns:
        # A perfect key is best
        if df[col].is_unique:
            candidate_keys.append(col)
            # Prioritize perfect non-numeric keys
            if not pd.api.types.is_numeric_dtype(df[col].dtype):
                return [col]

    # If no perfect key, find highly unique ones
    if candidate_keys:
        return candidate_keys[:1]  # Return the first one found

    for col in df.columns:
        uniqueness_ratio = df[col].nunique() / num_rows
        if uniqueness_ratio >= min_uniqueness_ratio:
            candidate_keys.append(col)

    return candidate_keys[:1]  # Return the best candidate found


def infer_datetime_columns(
    df: pd.DataFrame, sample_size: int = 1000, success_threshold: float = 0.9
) -> list[str]:
    """Infers which 'object' type columns are likely datetimes."""
    # --- THIS IS THE CORRECTED LINE ---
    # Use the general 'datetime64' to be compatible with modern pandas versions.
    datetime_cols = list(df.select_dtypes(include=["datetime64"]).columns)

    object_cols = df.select_dtypes(include=["object"]).columns
    for col in object_cols:
        sample = df[col].dropna().head(sample_size)
        if sample.empty:
            continue

        try:
            parsed_sample = pd.to_datetime(sample, errors="coerce")
            success_rate = parsed_sample.notna().sum() / len(sample)
            if success_rate >= success_threshold:
                datetime_cols.append(col)
        except (ValueError, TypeError):
            continue

    return list(set(datetime_cols))
