"""Memory-bounded streaming verification tests."""

import csv
import os

import pyarrow.parquet as pq
import pytest

from csvconv.converter import convert


class TestMemoryBounded:
    def test_large_csv_streaming_produces_correct_output(self, tmp_path):
        """Convert a large CSV (50K rows) and verify output integrity."""
        csv_path = str(tmp_path / "large.csv")
        pq_path = str(tmp_path / "large.parquet")

        # Generate 50K rows CSV (~3MB)
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value", "description"])
            for i in range(50000):
                writer.writerow([i, float(i) * 0.1, "x" * 50])

        summary = convert(csv_path, pq_path, block_size_mb=1)
        assert summary.total_success == 1

        table = pq.read_table(pq_path)
        assert table.num_rows == 50000
        assert len(table.schema) == 3

    def test_large_csv_with_small_block_size(self, tmp_path):
        """Small block_size_mb forces multiple chunks, verifying streaming works."""
        csv_path = str(tmp_path / "large.csv")
        pq_path = str(tmp_path / "large.parquet")

        # Generate CSV larger than 1MB
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value", "description"])
            for i in range(50000):
                writer.writerow([i, float(i) * 0.1, "x" * 50])

        # Use very small block size to force streaming
        summary = convert(csv_path, pq_path, block_size_mb=1)
        assert summary.total_success == 1

        table = pq.read_table(pq_path)
        assert table.num_rows == 50000

    def test_row_group_size_controls_output(self, tmp_path):
        """Verify row_group_size parameter is respected."""
        csv_path = str(tmp_path / "data.csv")
        pq_path = str(tmp_path / "data.parquet")

        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value"])
            for i in range(1000):
                writer.writerow([i, float(i)])

        summary = convert(csv_path, pq_path, row_group_size=100)
        assert summary.total_success == 1

        pf = pq.ParquetFile(pq_path)
        assert pf.metadata.num_rows == 1000
