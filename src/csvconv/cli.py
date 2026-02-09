"""CLI argument parsing and entry point for csvconv."""

import argparse
import logging
import sys

import csvconv
from csvconv import logging_config
from csvconv.converter import convert

logger = logging.getLogger("csvconv")


def _positive_int(value):
    # type: (str) -> int
    """Validate that a string represents a positive integer.

    Args:
        value: String value from argparse.

    Returns:
        Parsed integer if positive.

    Raises:
        argparse.ArgumentTypeError: If value is not a positive integer.
    """
    try:
        ivalue = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "invalid int value: '{}'".format(value)
        )
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(
            "value must be > 0, got {}".format(ivalue)
        )
    return ivalue


def _positive_float(value):
    # type: (str) -> float
    """Validate that a string represents a positive float.

    Args:
        value: String value from argparse.

    Returns:
        Parsed float if positive.

    Raises:
        argparse.ArgumentTypeError: If value is not a positive float.
    """
    try:
        fvalue = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "invalid float value: '{}'".format(value)
        )
    if fvalue <= 0:
        raise argparse.ArgumentTypeError(
            "value must be > 0, got {}".format(fvalue)
        )
    return fvalue


def parse_args(argv=None):
    # type: (list) -> argparse.Namespace
    """Parse command-line arguments for csvconv.

    Args:
        argv: List of argument strings. If None, uses sys.argv[1:].

    Returns:
        Parsed argparse.Namespace with all CLI options.
    """
    parser = argparse.ArgumentParser(
        prog="csvconv",
        description="Memory-efficient CSV and tar.gz to Parquet/CSV converter.",
    )

    parser.add_argument(
        "--version",
        action="version",
        version="csvconv {}".format(csvconv.__version__),
    )

    # Required arguments
    parser.add_argument(
        "--input",
        required=True,
        dest="input",
        help="Input file path (CSV or tar.gz)",
    )
    parser.add_argument(
        "--output",
        required=True,
        dest="output",
        help="Output file or directory path",
    )

    # Optional arguments
    parser.add_argument(
        "--input-type",
        choices=["csv", "tar.gz"],
        default=None,
        dest="input_type",
        help='Input type: "csv" or "tar.gz" (auto-detected from extension if not given)',
    )
    parser.add_argument(
        "--output-type",
        choices=["parquet", "csv"],
        default="parquet",
        dest="output_type",
        help='Output type: "parquet" or "csv" (default: parquet)',
    )
    parser.add_argument(
        "--block-size-mb",
        type=_positive_float,
        default=1,
        dest="block_size_mb",
        help="Block size in MB for CSV reading (default: 1, must be > 0)",
    )
    parser.add_argument(
        "--row-group-size",
        type=_positive_int,
        default=None,
        dest="row_group_size",
        help="Parquet row group size (default: None, must be > 0 if given)",
    )
    parser.add_argument(
        "--schema-sample-rows",
        type=_positive_int,
        default=1000,
        dest="schema_sample_rows",
        help="Number of rows to sample for schema inference (default: 1000, must be > 0)",
    )
    parser.add_argument(
        "--gzip",
        action="store_true",
        default=False,
        help="Enable gzip compression for CSV output",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARN", "WARNING", "ERROR"],
        default="INFO",
        dest="log_level",
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args(argv)

    # Auto-detect input type from extension if not explicitly provided
    if args.input_type is None:
        input_lower = args.input.lower()
        if input_lower.endswith(".tar.gz") or input_lower.endswith(".tgz"):
            args.input_type = "tar.gz"
        else:
            args.input_type = "csv"

    # Normalize WARN -> WARNING
    if args.log_level == "WARN":
        args.log_level = "WARNING"

    # Warn if --gzip used with --output-type parquet
    if args.gzip and args.output_type == "parquet":
        # Setup logging first so the warning is visible
        logging_config.setup_logging(args.log_level)
        logger.warning(
            "--gzip flag has no effect with --output-type parquet"
        )

    return args


def main(argv=None):
    # type: (list) -> int
    """Main entry point for csvconv CLI.

    Args:
        argv: List of argument strings. If None, uses sys.argv[1:].

    Returns:
        Exit code: 0 on success, 1 on failure.
    """
    try:
        args = parse_args(argv)
        logging_config.setup_logging(args.log_level)

        summary = convert(
            input_path=args.input,
            output_path=args.output,
            input_type=args.input_type,
            output_type=args.output_type,
            block_size_mb=args.block_size_mb,
            row_group_size=args.row_group_size,
            schema_sample_rows=args.schema_sample_rows,
            gzip=args.gzip,
        )

        print(summary.get_report())
        return 0

    except SystemExit:
        raise
    except Exception as e:
        logger.error("Conversion failed: %s", e)
        return 1
