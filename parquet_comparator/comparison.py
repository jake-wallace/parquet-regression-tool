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
    only joining and comparing on the intersection of columns.
    """

    before_cols = set(df_before.columns)
    after_cols = set(df_after.columns)
    common_cols = list(before_cols.intersection(after_cols))

    common_sort_keys = [key for key in sort_keys if key in common_cols]
    if not common_sort_keys:
        is_identical = schema_diff.is_identical
        return ComparisonData(is_identical=is_identical)

    df_before_casted = df_before.clone()
    df_after_casted = df_after.clone()
    for col_name in schema_diff.type_changes:
        if col_name in common_cols:
            df_before_casted = df_before_casted.with_columns(
                pl.col(col_name).cast(pl.Utf8)
            )
            df_after_casted = df_after_casted.with_columns(
                pl.col(col_name).cast(pl.Utf8)
            )

    merged = df_before_casted.select(common_cols).join(
        df_after_casted.select(common_cols),
        on=common_sort_keys,
        how="outer",
        suffix="_after",
    )

    after_cols_in_join = common_sort_keys + [
        f"{col}_after" for col in common_cols if col not in common_sort_keys
    ]
    rename_map_after = {
        f"{col}_after": col for col in common_cols if col not in common_sort_keys
    }
    added = (
        merged.filter(pl.col(common_cols[0]).is_null())
        .select(after_cols_in_join)
        .rename(rename_map_after)
    )

    deleted = merged.filter(pl.col(f"{common_cols[0]}_after").is_null()).select(
        common_cols
    )

    common_rows_df = merged.drop_nulls()
    modified_rows = []

    for col in common_cols:
        if col in common_sort_keys:
            continue

        col_before_name, col_after_name = col, f"{col}_after"
        col_before, col_after = pl.col(col_before_name), pl.col(col_after_name)

        diff_mask = None
        dtype = df_before_casted[col].dtype

        if dtype in pl.FLOAT_DTYPES and col not in schema_diff.type_changes:
            diff_mask = (col_before - col_after).abs() > tolerance
        else:
            diff_mask = col_before != col_after

        final_mask = diff_mask & (col_before.is_not_null() | col_after.is_not_null())

        differences = common_rows_df.filter(final_mask)
        if differences.height > 0:
            for row in differences.to_dicts():
                key_tuple = tuple(row[k] for k in common_sort_keys)
                key_str = str(key_tuple)

                modified_rows.append(
                    {
                        "key": key_str,
                        "column": col,
                        "value_before": str(row[col_before_name]),
                        "value_after": str(row[col_after_name]),
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
                "value_before": pl.Utf8,
                "value_after": pl.Utf8,
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
