"""Integration tests for tar.gz to Parquet conversion."""

import os

import pyarrow.parquet as pq
import pytest

from csvconv.converter import convert


class TestTargzToParquetIntegration:
    """End-to-end tar.gz -> Parquet conversion tests."""

    def test_targz_to_parquet_multiple_files(self, sample_targz, tmp_path):
        """3 CSVs in tar.gz should produce 3 Parquet files."""
        output_dir = str(tmp_path / "parquet_out")

        result = convert(
            input_path=sample_targz,
            output_path=output_dir,
            input_type="tar.gz",
            output_type="parquet",
        )

        assert result.total_success == 3
        assert result.total_failure == 0

        # Verify each parquet file exists and has data
        for idx in range(3):
            pq_path = os.path.join(output_dir, "data_{}.parquet".format(idx))
            assert os.path.exists(pq_path), "Missing: {}".format(pq_path)
            table = pq.read_table(pq_path)
            assert table.num_rows == 50
            assert table.column_names == ["id", "value", "name"]

    def test_targz_to_parquet_schema_mismatch_isolation(self, schema_mismatch_targz, tmp_path):
        """Schema mismatch in one member should not stop other conversions.

        The fixture has 3 files: file1.csv (matching schema), file2.csv
        (different schema), file3.csv (matching schema).
        Expect 2 successes and 1 failure.
        """
        output_dir = str(tmp_path / "parquet_out")

        result = convert(
            input_path=schema_mismatch_targz,
            output_path=output_dir,
            input_type="tar.gz",
            output_type="parquet",
        )

        assert result.total_success == 2
        assert result.total_failure == 1

        # Verify file1 and file3 were converted
        assert os.path.exists(os.path.join(output_dir, "file1.parquet"))
        assert os.path.exists(os.path.join(output_dir, "file3.parquet"))

        # file2 should not have been written (or cleaned up on error)
        failed_names = [f["file"] for f in result.failures]
        assert "file2.csv" in failed_names

    def test_targz_to_parquet_empty_archive(self, empty_targz, tmp_path):
        """tar.gz with no CSV members should produce 0 success/0 failure."""
        output_dir = str(tmp_path / "parquet_out")

        result = convert(
            input_path=empty_targz,
            output_path=output_dir,
            input_type="tar.gz",
            output_type="parquet",
        )

        assert result.total_success == 0
        assert result.total_failure == 0
