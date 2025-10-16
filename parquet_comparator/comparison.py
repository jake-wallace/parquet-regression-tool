import polars as pl
from dataclasses import dataclass, field


@dataclass
class ComparisonData:
    """A standard container for holding the results of a comparison."""

    added: pl.DataFrame = field(default_factory=pl.DataFrame)
    deleted: pl.DataFrame = field(default_factory=pl.DataFrame)
    modified: pl.DataFrame = field(default_factory=pl.DataFrame)
    is_identical: bool = True


def compare_dataframes_pl(
    df_before: pl.DataFrame, df_after: pl.DataFrame, sort_keys: list, tolerance: float
) -> ComparisonData:
    """
    Compares two Polars DataFrames using a unique key, categorizing differences
    into added, deleted, and modified.
    """

    # Outer join to find all differences
    merged = df_before.join(df_after, on=sort_keys, how="outer", suffix="_after")

    # For added rows, select ONLY the key columns and the '_after' columns, then rename them.
    after_cols = sort_keys + [col for col in df_after.columns if col not in sort_keys]
    added = (
        merged.filter(pl.col(df_before.columns[0]).is_null())
        .select(
            pl.col(
                sort_keys
                + [f"{col}_after" for col in after_cols if col not in sort_keys]
            )
        )
        .rename({f"{col}_after": col for col in after_cols if col not in sort_keys})
    )

    # For deleted rows, select ONLY the original columns from the 'before' dataframe.
    deleted = merged.filter(pl.col(df_after.columns[0] + "_after").is_null()).select(
        df_before.columns
    )

    common = merged.drop_nulls()
    modified_rows = []

    for col in df_before.columns:
        if col in sort_keys:
            continue

        col_before_name = col
        col_after_name = col + "_after"

        col_before = pl.col(col_before_name)
        col_after = pl.col(col_after_name)

        diff_mask = None
        dtype = df_before[col].dtype
        if dtype in pl.FLOAT_DTYPES:
            diff_mask = (col_before - col_after).abs() > tolerance
        else:
            diff_mask = col_before != col_after

        final_mask = diff_mask & (col_before.is_not_null() | col_after.is_not_null())

        differences = common.filter(final_mask)
        if differences.height > 0:
            for row in differences.to_dicts():

                # Proactively convert the key tuple to a string representation.
                # This safely handles any data type inside the key, including lists.
                key_tuple = tuple(row[k] for k in sort_keys)
                key_str = str(key_tuple)

                modified_rows.append(
                    {
                        "key": key_str,
                        "column": col,
                        "value_before": row[col_before_name],
                        "value_after": row[col_after_name],
                    }
                )

    modified_df = None
    if modified_rows:
        modified_df = pl.from_dicts(modified_rows)

    else:
        modified_df = pl.DataFrame(
            schema={
                "key": pl.Utf8,
                "column": pl.Utf8,
                "value_before": pl.Object,
                "value_after": pl.Object,
            }
        )

    is_identical = added.height == 0 and deleted.height == 0 and modified_df.height == 0
    return ComparisonData(
        added=added, deleted=deleted, modified=modified_df, is_identical=is_identical
    )
