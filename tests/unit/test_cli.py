"""Unit tests for CLI argument parsing."""

import pytest

from csvconv.cli import main, parse_args
import csvconv


class TestParseArgs:
    """Tests for parse_args function."""

    def test_parse_args_minimal(self):
        """Minimal required args should set correct defaults."""
        args = parse_args(["--input", "data.csv", "--output", "out.parquet"])
        assert args.input == "data.csv"
        assert args.output == "out.parquet"
        assert args.input_type == "csv"
        assert args.output_type == "parquet"
        assert args.block_size_mb == 1
        assert args.row_group_size is None
        assert args.schema_sample_rows == 1000
        assert args.gzip is False
        assert args.log_level == "INFO"

    def test_parse_args_all_options(self):
        """All options explicitly set should be parsed correctly."""
        args = parse_args([
            "--input", "archive.tar.gz",
            "--output", "/tmp/out",
            "--input-type", "tar.gz",
            "--output-type", "csv",
            "--block-size-mb", "64",
            "--row-group-size", "5000",
            "--schema-sample-rows", "2000",
            "--gzip",
            "--log-level", "DEBUG",
        ])
        assert args.input == "archive.tar.gz"
        assert args.output == "/tmp/out"
        assert args.input_type == "tar.gz"
        assert args.output_type == "csv"
        assert args.block_size_mb == 64
        assert args.row_group_size == 5000
        assert args.schema_sample_rows == 2000
        assert args.gzip is True
        assert args.log_level == "DEBUG"

    def test_parse_args_input_type_auto_detection(self):
        """Input type should auto-detect tar.gz from .tar.gz extension."""
        args = parse_args(["--input", "archive.tar.gz", "--output", "out/"])
        assert args.input_type == "tar.gz"

    def test_parse_args_tgz_auto_detection(self):
        """Input type should auto-detect tar.gz from .tgz extension."""
        args = parse_args(["--input", "archive.tgz", "--output", "out/"])
        assert args.input_type == "tar.gz"

    def test_parse_args_missing_required(self):
        """Missing required args should cause SystemExit."""
        with pytest.raises(SystemExit):
            parse_args([])

    def test_parse_args_log_level_warn_alias(self):
        """WARN should be normalized to WARNING."""
        args = parse_args([
            "--input", "data.csv",
            "--output", "out.parquet",
            "--log-level", "WARN",
        ])
        assert args.log_level == "WARNING"

    def test_parse_args_version_flag(self, capsys):
        """--version should print version and exit 0."""
        with pytest.raises(SystemExit) as exc_info:
            parse_args(["--version"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert csvconv.__version__ in captured.out

    def test_parse_args_block_size_mb_negative(self):
        """Negative block-size-mb should cause SystemExit."""
        with pytest.raises(SystemExit):
            parse_args([
                "--input", "data.csv",
                "--output", "out.parquet",
                "--block-size-mb", "-1",
            ])

    def test_parse_args_block_size_mb_zero(self):
        """Zero block-size-mb should cause SystemExit."""
        with pytest.raises(SystemExit):
            parse_args([
                "--input", "data.csv",
                "--output", "out.parquet",
                "--block-size-mb", "0",
            ])

    def test_parse_args_block_size_mb_valid(self):
        """Valid block-size-mb should be parsed correctly."""
        args = parse_args([
            "--input", "data.csv",
            "--output", "out.parquet",
            "--block-size-mb", "64",
        ])
        assert args.block_size_mb == 64

    def test_parse_args_row_group_size_negative(self):
        """Negative row-group-size should cause SystemExit."""
        with pytest.raises(SystemExit):
            parse_args([
                "--input", "data.csv",
                "--output", "out.parquet",
                "--row-group-size", "-100",
            ])

    def test_parse_args_row_group_size_zero(self):
        """Zero row-group-size should cause SystemExit."""
        with pytest.raises(SystemExit):
            parse_args([
                "--input", "data.csv",
                "--output", "out.parquet",
                "--row-group-size", "0",
            ])

    def test_parse_args_row_group_size_valid(self):
        """Valid row-group-size should be parsed correctly."""
        args = parse_args([
            "--input", "data.csv",
            "--output", "out.parquet",
            "--row-group-size", "10000",
        ])
        assert args.row_group_size == 10000

    def test_parse_args_schema_sample_rows_negative(self):
        """Negative schema-sample-rows should cause SystemExit."""
        with pytest.raises(SystemExit):
            parse_args([
                "--input", "data.csv",
                "--output", "out.parquet",
                "--schema-sample-rows", "-5",
            ])

    def test_parse_args_schema_sample_rows_zero(self):
        """Zero schema-sample-rows should cause SystemExit."""
        with pytest.raises(SystemExit):
            parse_args([
                "--input", "data.csv",
                "--output", "out.parquet",
                "--schema-sample-rows", "0",
            ])

    def test_parse_args_schema_sample_rows_valid(self):
        """Valid schema-sample-rows should be parsed correctly."""
        args = parse_args([
            "--input", "data.csv",
            "--output", "out.parquet",
            "--schema-sample-rows", "500",
        ])
        assert args.schema_sample_rows == 500

    def test_parse_args_gzip_flag(self):
        """--gzip flag should set gzip to True."""
        args = parse_args([
            "--input", "data.csv",
            "--output", "out/",
            "--output-type", "csv",
            "--gzip",
        ])
        assert args.gzip is True

    def test_parse_args_gzip_default_false(self):
        """Without --gzip flag, gzip should default to False."""
        args = parse_args([
            "--input", "data.csv",
            "--output", "out.parquet",
        ])
        assert args.gzip is False


class TestMain:
    """Tests for main() function."""

    def test_main_returns_zero_on_success(self, sample_csv, tmp_path):
        """Successful conversion should return exit code 0."""
        output = str(tmp_path / "output.parquet")
        result = main(["--input", sample_csv, "--output", output])
        assert result == 0

    def test_main_returns_nonzero_on_failure(self, tmp_path):
        """Nonexistent input file should return exit code 1."""
        nonexistent = str(tmp_path / "does_not_exist.csv")
        output = str(tmp_path / "output.parquet")
        result = main(["--input", nonexistent, "--output", output])
        assert result == 1
