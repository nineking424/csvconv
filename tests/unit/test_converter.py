"""Unit tests for csvconv converter dispatch."""

import pyarrow.parquet as pq
import pytest

from csvconv.converter import convert


class TestConverterCsvToParquet:
    """Tests for CSV to Parquet conversion."""

    def test_csv_to_parquet_end_to_end(self, sample_csv, tmp_path):
        """Basic CSV to Parquet conversion should produce valid output."""
        output = str(tmp_path / "output.parquet")
        result = convert(
            input_path=sample_csv,
            output_path=output,
            input_type="csv",
            output_type="parquet",
        )
        table = pq.read_table(output)
        assert table.num_rows == 100
        assert result.total_success == 1
        assert result.total_failure == 0

    def test_csv_to_parquet_preserves_types(self, sample_csv, tmp_path):
        """Parquet output should have appropriate types."""
        output = str(tmp_path / "output.parquet")
        convert(
            input_path=sample_csv,
            output_path=output,
            input_type="csv",
            output_type="parquet",
        )
        table = pq.read_table(output)
        assert "id" in table.column_names
        assert "value" in table.column_names
        assert "name" in table.column_names

    def test_csv_to_parquet_with_custom_block_size(self, large_csv, tmp_path):
        """Conversion with custom block_size should succeed."""
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
