import pandas as pd
import numpy as np
from dataclasses import dataclass, field


@dataclass
class ComparisonData:
    added: pd.DataFrame = field(default_factory=pd.DataFrame)
    deleted: pd.DataFrame = field(default_factory=pd.DataFrame)
    modified: pd.DataFrame = field(default_factory=pd.DataFrame)
    is_identical: bool = True


def compare_dataframes(
    df_before: pd.DataFrame, df_after: pd.DataFrame, sort_keys: list, tolerance: float
) -> ComparisonData:
    """
    Compares two dataframes, categorizing differences into added, deleted, and modified.
    """
    if sort_keys:
        df_before = df_before.set_index(sort_keys)
        df_after = df_after.set_index(sort_keys)

    # Outer join to find all differences
    merged = df_before.merge(
        df_after,
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=("_before", "_after"),
        indicator=True,
    )

    # Categorize Added and Deleted rows
    added = merged[merged["_merge"] == "right_only"].drop(
        columns=merged.filter(like="_before").columns.tolist() + ["_merge"]
    )
    added.columns = added.columns.str.replace("_after$", "", regex=True)

    deleted = merged[merged["_merge"] == "left_only"].drop(
        columns=merged.filter(like="_after").columns.tolist() + ["_merge"]
    )
    deleted.columns = deleted.columns.str.replace("_before$", "", regex=True)

    # Identify Modified rows
    common = merged[merged["_merge"] == "both"].drop(columns=["_merge"])
    modified_rows = []

    for col in df_before.columns:
        col_before, col_after = f"{col}_before", f"{col}_after"

        is_float = pd.api.types.is_float_dtype(common[col_before])

        if is_float:
            diff_mask = ~np.isclose(
                common[col_before], common[col_after], rtol=tolerance, equal_nan=True
            )
        else:
            diff_mask = common[col_before].ne(common[col_after]) & ~(
                common[col_before].isna() & common[col_after].isna()
            )

        if diff_mask.any():
            diff_indices = common.index[diff_mask]
            for idx in diff_indices:
                modified_rows.append(
                    {
                        "key": idx,
                        "column": col,
                        "value_before": common.loc[idx, col_before],
                        "value_after": common.loc[idx, col_after],
                    }
                )

    modified = pd.DataFrame(modified_rows)
    if not modified.empty:
        modified = modified.set_index("key")

    is_identical = added.empty and deleted.empty and modified.empty

    return ComparisonData(
        added=added, deleted=deleted, modified=modified, is_identical=is_identical
    )
