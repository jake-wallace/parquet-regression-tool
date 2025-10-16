import click
import yaml
from pathlib import Path
import datetime
from multiprocessing import Pool, cpu_count
from functools import partial

from parquet_comparator.discovery import pair_files
from parquet_comparator.tracking import ComparisonTracker, IDENTICAL_STATUSES
from parquet_comparator.main import ParquetComparator


def load_config(config_path: str) -> dict:
    """Loads the YAML configuration file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_rules_for_file(file_path: Path, config: dict) -> dict:
    """Loads global rules and overrides them with any matching file-specific rules."""
    rules = {
        "float_tolerance": config.get("float_tolerance", 1.0e-6),
        "ignore_columns": config.get("global_ignore_columns", []),
    }
    for specific_rule in config.get("file_specific_rules", []):
        if file_path.match(specific_rule["pattern"]):
            rules.update(specific_rule)
            return rules
    return rules


def worker(
    file_pair: tuple, config: dict, output_dir: Path, skip_checksum: bool
) -> tuple:
    """A single worker function to be run in parallel. Compares one pair of files."""
    file_before, file_after = file_pair
    relative_path = file_before.relative_to(Path(config["base_path_before"]))

    rules = get_rules_for_file(file_before, config)

    comparator = ParquetComparator(file_before, file_after, output_dir, config, rules)
    result = comparator.run(skip_checksum=skip_checksum)

    return (relative_path, result)


def find_unmatched_files(base_before: Path, base_after: Path) -> tuple[set, set]:
    """Finds files that exist in one directory but not the other."""
    files_before = {f.relative_to(base_before) for f in base_before.rglob("*.parquet")}
    files_after = {f.relative_to(base_after) for f in base_after.rglob("*.parquet")}

    only_in_before = files_before - files_after
    only_in_after = files_after - files_before

    return only_in_before, only_in_after


@click.command()
@click.option(
    "--config-file", "-c", default="config.yaml", help="Path to the configuration file."
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force re-comparison of all files, ignoring tracking log.",
)
@click.option(
    "--no-checksum",
    is_flag=True,
    help="Skip the checksum comparison and go straight to a detailed diff.",
)
@click.option(
    "--workers", "-w", default=cpu_count(), help="Number of parallel processes to use."
)
def main(config_file, force, no_checksum, workers):
    """A high-performance tool to compare Parquet files in parallel using Polars."""
    try:
        config = load_config(config_file)
    except FileNotFoundError:
        click.secho(f"Error: Configuration file not found at '{config_file}'", fg="red")
        return

    base_before = Path(config["base_path_before"])
    base_after = Path(config["base_path_after"])
    output_dir = Path(config["output_directory"])
    output_dir.mkdir(exist_ok=True)

    tracker = ComparisonTracker(output_dir / "comparison_log.db")

    start_time = datetime.datetime.now()
    click.secho(
        f"Starting comparison run at {start_time.strftime('%Y-%m-%d %H:%M:%S')}",
        bold=True,
    )
    click.secho(f"Using {workers} worker processes.", bold=True)

    all_pairs = list(pair_files(base_before, base_after))

    tasks_to_run = []
    if force:
        tasks_to_run = all_pairs
        click.secho(
            "(--force flag detected) Re-processing all file pairs.", fg="yellow"
        )
    else:
        click.secho("\n--- Pre-flight Check ---", bold=True)
        for pair in all_pairs:
            status = tracker.get_last_status(pair[0], pair[1])
            if status in IDENTICAL_STATUSES:
                click.secho(
                    f" -> Skipping '{pair[0].name}' (previously passed as {status})",
                    fg="green",
                )
                continue
            tasks_to_run.append(pair)

        skipped_count = len(all_pairs) - len(tasks_to_run)
        if skipped_count > 0:
            click.secho(
                f"\nSkipped {skipped_count} file pairs already processed and found identical.",
                fg="green",
            )

    if not tasks_to_run:
        click.secho("\nNo new or failed file pairs to compare.", fg="green")
    else:
        click.secho(
            f"\n--- Queued for Comparison ({len(tasks_to_run)} pairs) ---", bold=True
        )
        for task in tasks_to_run:
            click.echo(f"  - {task[0].relative_to(base_before)}")

        click.secho("\n--- Starting Parallel Processing ---", bold=True)
        worker_func = partial(
            worker, config=config, output_dir=output_dir, skip_checksum=no_checksum
        )

        with Pool(processes=workers) as pool:
            for relative_path, result in pool.imap_unordered(worker_func, tasks_to_run):
                click.secho(f"\nCompleted: {relative_path}", bold=True, fg="cyan")

                file_before = base_before / relative_path
                file_after = base_after / relative_path
                tracker.log_comparison(
                    file_before, file_after, result.status, result.report_path
                )

                if result.status in IDENTICAL_STATUSES:
                    click.secho(f"  -> Status: {result.status}", fg="green")
                    if result.report_path:
                        click.echo(
                            f"  -> Report generated (for audit): {result.report_path}"
                        )
                elif "DIFFERENCES_FOUND" in result.status:
                    click.secho(f"  -> Status: {result.status}", fg="yellow")
                    click.echo(f"  -> Report generated at: {result.report_path}")
                else:  # Error states
                    click.secho(f"  -> Status: {result.status}", fg="red")
                    if result.report_path:
                        click.echo(f"  -> Report generated at: {result.report_path}")
                    else:
                        click.echo(f"  -> Details: {result.details}")

    click.secho("\n--- Unmatched Files Summary ---", bold=True)
    only_in_before, only_in_after = find_unmatched_files(base_before, base_after)

    if not only_in_before and not only_in_after:
        click.secho(
            "All files in 'before' and 'after' directories were successfully paired.",
            fg="green",
        )
    else:
        if only_in_before:
            click.secho("\nFiles found in 'before' but NOT in 'after':", fg="yellow")
            for f in sorted(only_in_before):
                click.echo(f"  - {f}")
        if only_in_after:
            click.secho("\nFiles found in 'after' but NOT in 'before':", fg="yellow")
            for f in sorted(only_in_after):
                click.echo(f"  - {f}")

    end_time = datetime.datetime.now()
    click.secho(
        f"\nComparison run finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}",
        bold=True,
    )
    click.echo(f"Total duration: {end_time - start_time}")


if __name__ == "__main__":
    main()
