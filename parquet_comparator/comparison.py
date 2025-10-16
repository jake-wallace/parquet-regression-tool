import polars as pl
from dataclasses import dataclass, field
from .schemas import SchemaDiff


@dataclass
class ComparisonData:
    """A standard container for holding the results of a comparison."""

    added: pl.DataFrame = field(default_factory=pl.DataFrame)
    deleted: pl.DataFrame = field(default_factory=pl.DataFrame)
    modified: pl.DataFrame = field(default_factory=pl.DataFrame)
    is_identical: bool = True


def compare_dataframes_pl(
    df_before: pl.DataFrame,
    df_after: pl.DataFrame,
    sort_keys: list,
    tolerance: float,
    schema_diff: SchemaDiff,
) -> ComparisonData:
    """
    Compares two Polars DataFrames, handling schema differences gracefully by
    casting mismatched types to strings for comparison.
    """

    df_before_original_cols = df_before.columns
    df_after_original_cols = df_after.columns

    common_cols = set(df_before_original_cols).intersection(set(df_after_original_cols))

    for col_name in schema_diff.type_changes:
        if col_name in common_cols:
            df_before = df_before.with_columns(pl.col(col_name).cast(pl.Utf8))
            df_after = df_after.with_columns(pl.col(col_name).cast(pl.Utf8))

    join_cols = [c for c in df_before.columns if c in df_after.columns]

    merged = df_before.select(join_cols).join(
        df_after.select(join_cols), on=sort_keys, how="outer", suffix="_after"
    )

    after_cols_in_join = sort_keys + [
        f"{col}_after" for col in join_cols if col not in sort_keys
    ]
    rename_map_after = {
        f"{col}_after": col for col in join_cols if col not in sort_keys
    }
    added = (
        merged.filter(pl.col(join_cols[0]).is_null())
        .select(after_cols_in_join)
        .rename(rename_map_after)
    )

    deleted = merged.filter(pl.col(f"{join_cols[0]}_after").is_null()).select(join_cols)

    common = merged.drop_nulls()
    modified_rows = []

    for col in join_cols:
        if col in sort_keys:
            continue

        col_before_name, col_after_name = col, f"{col}_after"
        col_before, col_after = pl.col(col_before_name), pl.col(col_after_name)

        diff_mask = None
        dtype = df_before[col].dtype

        if dtype in pl.FLOAT_DTYPES and col not in schema_diff.type_changes:
            diff_mask = (col_before - col_after).abs() > tolerance
        else:
            diff_mask = col_before != col_after

        final_mask = diff_mask & (col_before.is_not_null() | col_after.is_not_null())

        differences = common.filter(final_mask)
        if differences.height > 0:
            for row in differences.to_dicts():
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

    is_identical = (
        added.height == 0
        and deleted.height == 0
        and modified_df.height == 0
        and schema_diff.is_identical
    )
    return ComparisonData(
        added=added, deleted=deleted, modified=modified_df, is_identical=is_identical
    )
