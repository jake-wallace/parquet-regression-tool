import polars as pl


def infer_sort_keys_pl(
    df: pl.DataFrame, min_uniqueness_ratio: float = 0.99
) -> list[str]:
    candidate_keys = []
    if df.height == 0:
        return []

    for col in df.columns:
        if df[col].n_unique() == df.height:
            candidate_keys.append(col)
            if df[col].dtype not in pl.NUMERIC_DTYPES:
                return [col]

    if candidate_keys:
        return candidate_keys[:1]

    for col in df.columns:
        if (df[col].n_unique() / df.height) >= min_uniqueness_ratio:
            candidate_keys.append(col)

    return candidate_keys[:1]


def infer_datetime_columns_pl(
    df: pl.DataFrame, sample_size: int = 1000, success_threshold: float = 0.9
) -> list[str]:
    datetime_cols = df.select(pl.col(pl.Datetime)).columns

    string_cols = df.select(pl.col(pl.Utf8)).columns
    for col in string_cols:
        sample = df[col].drop_nulls().head(sample_size)
        if sample.height == 0:
            continue

        parsed_sample = sample.str.to_datetime(strict=False, errors="null")
        success_rate = parsed_sample.is_not_null().sum() / sample.height
        if success_rate >= success_threshold:
            datetime_cols.append(col)

    return list(set(datetime_cols))
