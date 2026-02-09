"""Unit tests for csvconv CSV streaming reader."""

import os
import pyarrow as pa
import pytest

from csvconv.reader.csv_reader import read_streaming


class TestReadStreaming:
    """Tests for read_streaming function."""

    def test_read_streaming_yields_record_batches(self, sample_csv):
        """read_streaming should yield RecordBatch objects."""
        batches = list(read_streaming(sample_csv))
        assert len(batches) > 0
        for batch in batches:
            assert isinstance(batch, pa.RecordBatch)

    def test_read_streaming_preserves_all_data(self, sample_csv):
        """Concatenated batches should contain all 100 rows."""
        batches = list(read_streaming(sample_csv))
        total_rows = sum(batch.num_rows for batch in batches)
        assert total_rows == 100

    def test_read_streaming_has_correct_columns(self, sample_csv):
        """Batches should have id, value, name columns."""
        batches = list(read_streaming(sample_csv))
        assert batches[0].schema.names == ["id", "value", "name"]

    def test_read_streaming_respects_block_size(self, large_csv):
        """With small block_size, should yield multiple batches."""
        batches = list(read_streaming(large_csv, block_size_mb=1))
        assert len(batches) > 1

    def test_read_streaming_with_explicit_schema(self, sample_csv):
        """When schema is provided, all batches should conform."""
        schema = pa.schema([
            ("id", pa.int64()),
            ("value", pa.float64()),
            ("name", pa.string()),
        ])
        batches = list(read_streaming(sample_csv, schema=schema))
        for batch in batches:
            assert batch.schema.equals(schema)

    def test_read_streaming_empty_file(self, empty_csv):
        """Empty CSV (header only) should yield zero batches."""
        batches = list(read_streaming(empty_csv))
        assert len(batches) == 0

    def test_read_streaming_file_not_found(self):
        """Non-existent path should raise an error."""
        with pytest.raises((FileNotFoundError, OSError, pa.ArrowInvalid)):
            list(read_streaming("/nonexistent/path.csv"))

    def test_read_streaming_from_file_object(self, sample_csv):
        """Should work with file-like objects (BinaryIO)."""
        with open(sample_csv, "rb") as f:
            batches = list(read_streaming(f))
        total_rows = sum(b.num_rows for b in batches)
        assert total_rows == 100

    def test_read_streaming_block_size_conversion(self, sample_csv):
        """block_size_mb=2 should be converted to 2*1024*1024 bytes internally."""
        # Just verify it doesn't crash with different block sizes
        batches_1 = list(read_streaming(sample_csv, block_size_mb=1))
        batches_2 = list(read_streaming(sample_csv, block_size_mb=2))
        rows_1 = sum(b.num_rows for b in batches_1)
        rows_2 = sum(b.num_rows for b in batches_2)
        assert rows_1 == rows_2 == 100
