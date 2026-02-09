"""Integration tests for CLI using subprocess."""

import gzip
import os
import subprocess
import sys

import pyarrow.parquet as pq
import pytest

import csvconv


class TestCliIntegration:
    """End-to-end CLI tests via subprocess."""

    def test_cli_csv_to_parquet_subprocess(self, sample_csv, tmp_path):
        """CLI should convert CSV to Parquet with exit code 0."""
        output = str(tmp_path / "output.parquet")
        result = subprocess.run(
            [sys.executable, "-m", "csvconv",
             "--input", sample_csv, "--output", output],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, "stderr: {}".format(result.stderr)
        assert os.path.exists(output)
        table = pq.read_table(output)
        assert table.num_rows == 100

    def test_cli_targz_to_parquet_subprocess(self, sample_targz, tmp_path):
        """CLI should convert tar.gz to Parquet files with exit code 0."""
        output_dir = str(tmp_path / "parquet_out")
        result = subprocess.run(
            [sys.executable, "-m", "csvconv",
             "--input", sample_targz,
             "--output", output_dir,
             "--output-type", "parquet"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, "stderr: {}".format(result.stderr)
        assert os.path.isdir(output_dir)

        # Verify parquet files were created
        parquet_files = [
            f for f in os.listdir(output_dir)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) == 3

    def test_cli_targz_to_csv_gzip_subprocess(self, sample_targz, tmp_path):
        """CLI should extract tar.gz to gzipped CSV files."""
        output_dir = str(tmp_path / "csv_out")
        result = subprocess.run(
            [sys.executable, "-m", "csvconv",
             "--input", sample_targz,
             "--output", output_dir,
             "--output-type", "csv",
             "--gzip"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, "stderr: {}".format(result.stderr)
        assert os.path.isdir(output_dir)

        # Verify .csv.gz files were created and are valid gzip
        gz_files = [
            f for f in os.listdir(output_dir)
            if f.endswith(".csv.gz")
        ]
        assert len(gz_files) == 3

        for gz_file in gz_files:
            gz_path = os.path.join(output_dir, gz_file)
            with gzip.open(gz_path, "rb") as f:
                content = f.read().decode("utf-8")
            assert "id,value,name" in content

    def test_cli_error_exit_code(self, tmp_path):
        """Nonexistent input should produce exit code 1 and stderr output."""
        nonexistent = str(tmp_path / "does_not_exist.csv")
        output = str(tmp_path / "output.parquet")
        result = subprocess.run(
            [sys.executable, "-m", "csvconv",
             "--input", nonexistent, "--output", output],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert result.stderr != ""

    def test_cli_version_flag(self):
        """--version should print version and exit 0."""
        result = subprocess.run(
            [sys.executable, "-m", "csvconv", "--version"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert csvconv.__version__ in result.stdout

    def test_cli_warn_log_level_accepted(self, sample_csv, tmp_path):
        """--log-level WARN should be accepted and produce exit code 0."""
        output = str(tmp_path / "output.parquet")
        result = subprocess.run(
            [sys.executable, "-m", "csvconv",
             "--input", sample_csv, "--output", output,
             "--log-level", "WARN"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, "stderr: {}".format(result.stderr)

    def test_cli_help_flag(self):
        """--help should print usage info and exit 0."""
        result = subprocess.run(
            [sys.executable, "-m", "csvconv", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "usage" in result.stdout.lower() or "csvconv" in result.stdout
