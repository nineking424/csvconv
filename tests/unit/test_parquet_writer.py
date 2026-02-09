"""Unit tests for csvconv incremental Parquet writer."""

import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from csvconv.writer.parquet_writer import IncrementalParquetWriter


def _make_batch(schema, num_rows=100):
    """Create a test RecordBatch."""
    data = {
        "id": list(range(num_rows)),
        "value": [float(i) * 1.5 for i in range(num_rows)],
        "name": ["name_{}".format(i) for i in range(num_rows)],
    }
    return pa.RecordBatch.from_pydict(data, schema=schema)


@pytest.fixture
def test_schema():
    return pa.schema([
        ("id", pa.int64()),
        ("value", pa.float64()),
        ("name", pa.string()),
    ])


class TestIncrementalParquetWriter:
    """Tests for IncrementalParquetWriter."""

    def test_write_single_batch_creates_parquet(self, tmp_path, test_schema):
        """Writing a single batch should create a valid Parquet file."""
        output = str(tmp_path / "out.parquet")
        batch = _make_batch(test_schema, 50)

        writer = IncrementalParquetWriter(output, test_schema)
        writer.write_batch(batch)
        writer.close()

        table = pq.read_table(output)
        assert table.num_rows == 50

    def test_write_multiple_batches_preserves_data(self, tmp_path, test_schema):
        """Multiple batches should all be preserved in the output."""
        output = str(tmp_path / "out.parquet")
        writer = IncrementalParquetWriter(output, test_schema)

        for i in range(5):
            batch = _make_batch(test_schema, 100)
            writer.write_batch(batch)
        writer.close()

        table = pq.read_table(output)
        assert table.num_rows == 500

    def test_row_group_size_controls_flush(self, tmp_path, test_schema):
        """row_group_size should control how many rows per row group."""
        output = str(tmp_path / "out.parquet")
        writer = IncrementalParquetWriter(output, test_schema, row_group_size=200)

        for _ in range(5):
            batch = _make_batch(test_schema, 100)
            writer.write_batch(batch)
        writer.close()

        pf = pq.ParquetFile(output)
        assert pf.metadata.num_row_groups >= 2

    def test_writer_context_manager(self, tmp_path, test_schema):
        """Writer should work as a context manager."""
        output = str(tmp_path / "out.parquet")
        batch = _make_batch(test_schema, 50)

        with IncrementalParquetWriter(output, test_schema) as writer:
            writer.write_batch(batch)

        table = pq.read_table(output)
        assert table.num_rows == 50

    def test_write_to_nonexistent_directory_raises(self, test_schema):
        """Writing to a non-existent directory should raise an error."""
        with pytest.raises((OSError, FileNotFoundError)):
            IncrementalParquetWriter("/nonexistent/dir/out.parquet", test_schema)
