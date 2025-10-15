import click
import yaml
from pathlib import Path
import datetime
from parquet_comparator.discovery import pair_files
from parquet_comparator.tracking import ComparisonTracker
from parquet_comparator.main import ParquetComparator


def load_config(config_path: str) -> dict:
    """Loads the YAML configuration file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_global_rules(config: dict) -> dict:
    """Loads global, non-schema-specific rules from the config."""
    return {
        "float_tolerance": config.get("float_tolerance", 1.0e-6),
        "ignore_columns": config.get("global_ignore_columns", []),
    }


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
def main(config_file, force, no_checksum):
    """
    A generic tool to compare Parquet files for regression testing.
    It automatically infers keys and schemas, performs a fast checksum check,
    and falls back to a detailed diff, generating console and HTML reports.
    """
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
        f"Starting comparison at {start_time.strftime('%Y-%m-%d %H:%M:%S')}", bold=True
    )
    click.echo(f"Comparing '{base_before}' vs '{base_after}'")

    file_pairs = list(pair_files(base_before, base_after))
    if not file_pairs:
        click.secho("No matching file pairs found.", fg="yellow")
        return

    # Load the global rules once
    global_rules = get_global_rules(config)

    for file_before, file_after in file_pairs:
        relative_path = file_before.relative_to(base_before)
        click.secho(f"\nProcessing: {relative_path}", bold=True, fg="cyan")

        if not force and tracker.has_been_processed(file_before, file_after):
            click.secho(
                "  -> Already processed and logged as identical. Skipping.", fg="green"
            )
            continue

        comparator = ParquetComparator(
            file_before, file_after, output_dir, config, global_rules
        )
        result = comparator.run(skip_checksum=no_checksum)

        tracker.log_comparison(
            file_before, file_after, result.status, result.report_path
        )

        if "IDENTICAL" in result.status:
            click.secho(f"  -> Status: {result.status}", fg="green")
        elif result.status == "DIFFERENCES_FOUND":
            click.secho(f"  -> Status: {result.status}", fg="yellow")
            click.echo(f"  -> Report generated at: {result.report_path}")
        else:
            click.secho(f"  -> Status: {result.status}", fg="red")
            click.echo(f"  -> Details: {result.details}")

    end_time = datetime.datetime.now()
    click.secho(
        f"\nComparison finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}", bold=True
    )
    click.echo(f"Total duration: {end_time - start_time}")


if __name__ == "__main__":
    main()
