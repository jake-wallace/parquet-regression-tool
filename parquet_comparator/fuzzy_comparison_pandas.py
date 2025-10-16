import pandas as pd
import jellyfish
import click
import numpy as np

# ComparisonData class here to avoid circular imports
# and keep this module self-contained as a helper.
from dataclasses import dataclass, field


@dataclass
class PandasComparisonData:
    added: pd.DataFrame = field(default_factory=pd.DataFrame)
    deleted: pd.DataFrame = field(default_factory=pd.DataFrame)
    modified: pd.DataFrame = field(default_factory=pd.DataFrame)
    is_identical: bool = True


def _calculate_row_similarity(row1: pd.Series, row2: pd.Series, weights: dict) -> float:
    """Calculates a weighted similarity score between two rows (pandas Series)."""
    weighted_scores = []
    total_weight = 0.0

    for col, val1 in row1.items():
        val2 = row2.get(col)
        weight = weights.get(col, 1.0)

        score = 0.0
        if pd.isna(val1) and pd.isna(val2):
            score = 1.0
        elif not pd.isna(val1) and not pd.isna(val2):
            if isinstance(val1, str) and isinstance(val2, str):
                score = jellyfish.jaro_winkler_similarity(val1, val2)
            elif val1 == val2:
                score = 1.0

        weighted_scores.append(score * weight)
        total_weight += weight

    return sum(weighted_scores) / total_weight if total_weight > 0 else 0.0


def _get_column_weights(df: pd.DataFrame) -> dict:
    """Generates weights for each column based on its cardinality."""
    weights = {}
    num_rows = len(df)
    if num_rows == 0:
        return weights
    for col in df.columns:
        cardinality_ratio = df[col].nunique() / num_rows
        weights[col] = 1.0 + cardinality_ratio
    return weights


def _find_best_blocking_column(df: pd.DataFrame) -> str | None:
    """Finds a good column to "block" on to reduce the search space."""
    potential_cols = df.select_dtypes(exclude=["number", "datetime"]).columns
    if potential_cols.empty:
        return None

    cardinalities = {
        col: df[col].nunique() / len(df) for col in potential_cols if len(df) > 0
    }

    candidates = {
        col: ratio for col, ratio in cardinalities.items() if 0.1 < ratio < 0.95
    }
    if not candidates:
        fallback_candidates = {
            col: ratio for col, ratio in cardinalities.items() if ratio < 0.99
        }
        if not fallback_candidates:
            return None
        return max(fallback_candidates, key=fallback_candidates.get)

    return max(candidates, key=candidates.get)


def fuzzy_compare_dataframes(
    df_before: pd.DataFrame, df_after: pd.DataFrame, similarity_threshold: float
) -> PandasComparisonData:
    """Performs a fuzzy, weighted, row-by-row comparison using pandas."""
    modified_rows = []
    matched_after_indices = set()

    df_before_copy = df_before.copy().reset_index(drop=True)
    df_after_copy = df_after.copy().reset_index(drop=True)

    blocking_col = _find_best_blocking_column(df_before_copy)
    if blocking_col:
        click.echo(
            f"  -> Using '{blocking_col}' as a blocking column to speed up fuzzy matching."
        )
        df_after_grouped = dict(list(df_after_copy.groupby(blocking_col)))
    else:
        click.secho(
            "  -> No suitable blocking column found. Fuzzy matching may be slow on large files.",
            fg="yellow",
        )
        df_after_grouped = {None: df_after_copy}

    column_weights = _get_column_weights(df_before_copy)
    click.echo(
        f"  -> Using column weights for scoring: {{ {', '.join(f'{k}: {v:.2f}' for k, v in column_weights.items())} }}"
    )

    matched_before_indices = set()

    for index_b, row_b in df_before_copy.iterrows():
        best_match_index = -1
        highest_score = 0.0

        block_key = row_b.get(blocking_col)
        search_df = df_after_grouped.get(block_key, pd.DataFrame())

        if not search_df.empty:
            for index_a, row_a in search_df.iterrows():
                if index_a in matched_after_indices:
                    continue

                score = _calculate_row_similarity(row_b, row_a, column_weights)
                if score > highest_score:
                    highest_score = score
                    best_match_index = index_a

        if highest_score >= similarity_threshold:
            matched_after_indices.add(best_match_index)
            matched_before_indices.add(index_b)

            row_a_best_match = df_after_copy.loc[best_match_index]

            if highest_score < 1.0:
                for col in df_before_copy.columns:
                    val_b, val_a = row_b[col], row_a_best_match[col]

                    is_diff = (val_b != val_a) and not (
                        pd.isna(val_b) and pd.isna(val_a)
                    )
                    if isinstance(val_b, float) and isinstance(val_a, float):
                        is_diff = not np.isclose(val_b, val_a)

                    if is_diff:
                        modified_rows.append(
                            {
                                "key": f"Fuzzy Match (Score: {highest_score:.3f})",
                                "column": col,
                                "value_before": val_b,
                                "value_after": val_a,
                            }
                        )

    unmatched_before_indices = set(df_before_copy.index) - matched_before_indices
    unmatched_after_indices = set(df_after_copy.index) - matched_after_indices

    deleted_df = df_before_copy.loc[list(unmatched_before_indices)]
    added_df = df_after_copy.loc[list(unmatched_after_indices)]
    modified_df = pd.DataFrame(modified_rows)
    if not modified_df.empty:
        modified_df.set_index("key", inplace=True)

    is_identical = added_df.empty and deleted_df.empty and modified_df.empty

    return PandasComparisonData(
        added=added_df,
        deleted=deleted_df,
        modified=modified_df,
        is_identical=is_identical,
    )
