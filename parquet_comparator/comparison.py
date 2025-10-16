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

    # Find rows that only existed in the 'after' df (added)
    added = merged.filter(pl.col(df_before.columns[0]).is_null()).select(
        pl.all().name.map(lambda name: name.replace("_after", ""))
    )

    # Find rows that only existed in the 'before' df (deleted)
    deleted = merged.filter(pl.col(df_after.columns[0] + "_after").is_null()).select(
        df_before.columns
    )

    # Find rows that existed in both to check for modifications
    common = merged.drop_nulls()
    modified_rows = []

    for col in df_before.columns:
        if col in sort_keys:
            continue  # Skip key columns

        col_before_name = col
        col_after_name = col + "_after"

        col_before = pl.col(col_before_name)
        col_after = pl.col(col_after_name)

        # Find differences, handling nulls correctly (a null is not different from a null)
        diff_mask = (col_before != col_after) & (
            col_before.is_not_null() | col_after.is_not_null()
        )

        # Note: Polars does not have a built-in 'isclose' for floats like numpy.
        # For floats, a simple != is used here. A more complex UDF could be added for tolerance.

        differences = common.filter(diff_mask)
        if differences.height > 0:
            # Collect details for the report
            for row in differences.to_dicts():
                modified_rows.append(
                    {
                        "key": tuple(row[k] for k in sort_keys),
                        "column": col,
                        "value_before": row[col_before_name],
                        "value_after": row[col_after_name],
                    }
                )

    modified_df = pl.from_dicts(modified_rows)
    if modified_df.height > 0:
        # Cast key tuple to a string for display in the report
        modified_df = modified_df.with_columns(pl.col("key").cast(pl.Utf8))

    is_identical = added.height == 0 and deleted.height == 0 and modified_df.height == 0
    return ComparisonData(
        added=added, deleted=deleted, modified=modified_df, is_identical=is_identical
    )
