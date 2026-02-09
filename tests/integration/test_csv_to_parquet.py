"""Integration tests for CSV to Parquet conversion."""

import pyarrow.parquet as pq
import pytest

from csvconv.converter import convert


class TestCsvToParquetIntegration:
    """End-to-end CSV to Parquet tests."""

    def test_csv_to_parquet_end_to_end(self, sample_csv, tmp_path):
        """Full pipeline: CSV file -> Parquet file."""
        output = str(tmp_path / "output.parquet")
        result = convert(
            input_path=sample_csv,
            output_path=output,
            input_type="csv",
            output_type="parquet",
        )
        table = pq.read_table(output)
        assert table.num_rows == 100
        assert table.column_names == ["id", "value", "name"]
        assert result.total_success == 1

    def test_csv_to_parquet_preserves_types(self, sample_csv, tmp_path):
        """Parquet should preserve numeric and string types."""
        output = str(tmp_path / "output.parquet")
        convert(input_path=sample_csv, output_path=output, input_type="csv", output_type="parquet")
        table = pq.read_table(output)
        # Check that values are readable and correct
        ids = table.column("id").to_pylist()
        assert ids[0] == 0
        assert ids[-1] == 99

    def test_csv_to_parquet_with_custom_block_size(self, large_csv, tmp_path):
        """Custom block_size should not affect output correctness."""
        output = str(tmp_path / "output.parquet")
        result = convert(
            input_path=large_csv,
            output_path=output,
            input_type="csv",
            output_type="parquet",
            block_size_mb=2,
        )
        table = pq.read_table(output)
        assert table.num_rows == 50000
        assert result.total_success == 1

    def test_csv_to_parquet_with_row_group_size(self, large_csv, tmp_path):
        """row_group_size should be reflected in Parquet metadata."""
        output = str(tmp_path / "output.parquet")
        convert(
            input_path=large_csv,
            output_path=output,
            input_type="csv",
            output_type="parquet",
            row_group_size=10000,
        )
        pf = pq.ParquetFile(output)
        assert pf.metadata.num_row_groups >= 2

    def test_csv_to_parquet_empty_csv(self, empty_csv, tmp_path):
        """Empty CSV should produce success without error."""
        output = str(tmp_path / "output.parquet")
        result = convert(
            input_path=empty_csv,
            output_path=output,
            input_type="csv",
            output_type="parquet",
        )
        assert result.total_success == 1
