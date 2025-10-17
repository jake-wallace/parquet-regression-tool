import polars as pl
import jellyfish
import click

from .comparison import ComparisonData


def _get_column_weights(df: pl.DataFrame) -> dict:
    """Generates weights for each column based on its cardinality."""
    weights = {}
    if df.height == 0:
        return weights
    for col in df.columns:
        if col.startswith("_id"):
            continue
        weights[col] = 1.0 + (df[col].n_unique() / df.height)
    return weights


def _find_best_blocking_column(df: pl.DataFrame) -> str | None:
    """Finds a good column to "block" on."""
    potential_cols = df.select(pl.col(pl.Utf8)).columns
    if not potential_cols:
        return None
    cardinalities = {
        col: df[col].n_unique() / len(df) for col in potential_cols if len(df) > 0
    }
    candidates = {col: r for col, r in cardinalities.items() if 0.1 < r < 0.95}
    if not candidates:
        fallback = {
            col: r
            for col, r in cardinalities.items()
            if r < 0.99 and not col.startswith("_id")
        }
        return max(fallback, key=fallback.get) if fallback else None
    return max(candidates, key=candidates.get)


def fuzzy_compare_dataframes_pl(
    df_before: pl.DataFrame, df_after: pl.DataFrame, similarity_threshold: float
) -> ComparisonData:
    """Performs a fuzzy, weighted, row-by-row comparison using a pure Polars join-based approach."""
    if df_before.height == 0 and df_after.height == 0:
        return ComparisonData(is_identical=True)
    if df_before.height == 0:
        return ComparisonData(added=df_after, is_identical=False)
    if df_after.height == 0:
        return ComparisonData(deleted=df_before, is_identical=False)

    df_before = df_before.with_row_count("_id_before")
    df_after = df_after.with_row_count("_id_after")

    weights = _get_column_weights(df_before)
    blocking_col = _find_best_blocking_column(df_before)

    candidate_pairs = None
    if blocking_col:
        click.echo(
            f"  -> Using '{blocking_col}' as a blocking column to speed up fuzzy matching."
        )
        base_join = df_before.join(
            df_after, on=blocking_col, how="inner", suffix="_after"
        )
        if (
            blocking_col in base_join.columns
            and f"{blocking_col}_after" not in base_join.columns
        ):
            candidate_pairs = base_join.with_columns(
                pl.col(blocking_col).alias(f"{blocking_col}_after")
            )
        else:
            candidate_pairs = base_join
    else:
        click.secho(
            "  -> No suitable blocking column found. Using cross join, which may be slow.",
            fg="yellow",
        )
        df_after_renamed = df_after.rename(
            {col: f"{col}_after" for col in df_after.columns}
        )
        candidate_pairs = df_before.join(df_after_renamed, how="cross")

    if candidate_pairs.height == 0:
        return ComparisonData(
            deleted=df_before.drop("_id_before"),
            added=df_after.drop("_id_after"),
            is_identical=False,
        )

    score_expressions = []
    total_weight = sum(weights.values())
    for col in weights:
        weight = weights[col]
        col_after = f"{col}_after"

        string_similarity_expr = pl.struct([col, col_after]).map_elements(
            lambda s, c=col, ca=col_after: jellyfish.jaro_winkler_similarity(
                str(s.get(c)), str(s.get(ca))
            ),
            return_dtype=pl.Float64,
        )

        expr = (
            pl.when(pl.col(col).is_null() & pl.col(col_after).is_null())
            .then(1.0)
            .when(pl.col(col).is_not_null() & pl.col(col_after).is_not_null())
            .then(
                string_similarity_expr
                if df_before.schema.get(col) == pl.Utf8
                else (pl.col(col) == pl.col(col_after)).cast(pl.Float64)
            )
            .otherwise(0.0)
        )

        score_expressions.append((expr * weight).alias(f"{col}_score"))

    scored_pairs = candidate_pairs.with_columns(
        (pl.sum_horizontal(score_expressions) / total_weight).alias("similarity_score")
    )

    best_matches = (
        scored_pairs.sort("similarity_score", descending=True)
        .group_by("_id_before")
        .first()
    )

    strong_matches = best_matches.filter(
        pl.col("similarity_score") >= similarity_threshold
    )

    matched_before_ids = strong_matches["_id_before"]
    matched_after_ids = strong_matches["_id_after"]

    deleted = df_before.filter(~pl.col("_id_before").is_in(matched_before_ids)).drop(
        "_id_before"
    )
    added = df_after.filter(~pl.col("_id_after").is_in(matched_after_ids)).drop(
        "_id_after"
    )

    modified_pairs = strong_matches.filter(pl.col("similarity_score") < 1.0)
    modified_rows = []

    if modified_pairs.height > 0:
        for row in modified_pairs.to_dicts():
            for col in weights:
                val_b = row[col]
                val_a = row.get(f"{col}_after")
                if val_b != val_a and not (val_b is None and val_a is None):
                    modified_rows.append(
                        {
                            "key": f"Fuzzy Match (Score: {row['similarity_score']:.3f})",
                            "column": col,
                            "value_before": str(val_b),
                            "value_after": str(val_a),
                        }
                    )

    # --- THIS IS THE DEFINITIVE FIX FOR THE NODATAERROR ---
    modified = None
    if modified_rows:
        modified = pl.from_dicts(modified_rows)
    else:
        # If there are no modifications, create an EMPTY DataFrame with the correct schema.
        modified = pl.DataFrame(
            schema={
                "key": pl.Utf8,
                "column": pl.Utf8,
                "value_before": pl.Utf8,
                "value_after": pl.Utf8,
            }
        )
    # --- END OF FIX ---

    is_identical = added.height == 0 and deleted.height == 0 and modified.height == 0
    return ComparisonData(
        added=added, deleted=deleted, modified=modified, is_identical=is_identical
    )
