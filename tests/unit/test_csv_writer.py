"""Unit tests for csvconv CSV writer."""

import gzip
import inspect
import io
import os

import pyarrow as pa
import pyarrow.csv as pcsv
import pytest

from csvconv.writer.csv_writer import extract_stream, write_csv


def _make_batch(num_rows=100):
    """Create a test RecordBatch with id/value/name columns."""
    schema = pa.schema([
        ("id", pa.int64()),
        ("value", pa.float64()),
        ("name", pa.string()),
    ])
    data = {
        "id": list(range(num_rows)),
        "value": [float(i) * 1.5 for i in range(num_rows)],
        "name": ["name_{}".format(i) for i in range(num_rows)],
    }
    return pa.RecordBatch.from_pydict(data, schema=schema)


class TestExtractStream:
    """Tests for extract_stream (raw byte-fidelity copy)."""

    def test_extract_stream_copies_bytes_exactly(self, tmp_path):
        """Extracted file must be byte-for-byte identical to source."""
        original = b"id,value,name\n1,1.5,alice\n2,3.0,bob\n"
        source = io.BytesIO(original)
        output = str(tmp_path / "out.csv")

        extract_stream(source, output)

        with open(output, "rb") as f:
            assert f.read() == original

    def test_extract_stream_with_gzip_compression(self, tmp_path):
        """gzip_compress=True should produce a valid gzip that decompresses to original."""
        original = b"id,value,name\n1,1.5,alice\n2,3.0,bob\n"
        source = io.BytesIO(original)
        output = str(tmp_path / "out.csv.gz")

        extract_stream(source, output, gzip_compress=True)

        with gzip.open(output, "rb") as f:
            assert f.read() == original

    def test_extract_stream_empty_input(self, tmp_path):
        """Empty stream should produce an empty file."""
        source = io.BytesIO(b"")
        output = str(tmp_path / "empty.csv")

        extract_stream(source, output)

        assert os.path.getsize(output) == 0

    def test_extract_stream_large_data_streams(self, tmp_path):
        """10MB stream should be extracted correctly."""
        # 10MB of data
        chunk = b"x" * 1024
        original = chunk * (10 * 1024)  # 10MB
        source = io.BytesIO(original)
        output = str(tmp_path / "large.csv")

        extract_stream(source, output)

        with open(output, "rb") as f:
            result = f.read()
        assert len(result) == len(original)
        assert result == original

    def test_extract_stream_uses_atomic_write(self, tmp_path, mocker):
        """os.replace should be called for atomic write pattern."""
        mock_replace = mocker.patch("csvconv.writer.csv_writer.os.replace", wraps=os.replace)

        source = io.BytesIO(b"id,value\n1,2\n")
        output = str(tmp_path / "atomic.csv")

        extract_stream(source, output)

        mock_replace.assert_called_once()
        # Verify the final path is the expected output
        call_args = mock_replace.call_args
        assert call_args[0][1] == output


class TestWriteCsv:
    """Tests for write_csv (RecordBatch to CSV)."""

    def test_write_csv_from_batches(self, tmp_path):
        """RecordBatches should be written as valid CSV content."""
        batch = _make_batch(10)
        output = str(tmp_path / "out.csv")

        write_csv(iter([batch]), output)

        # Read back and verify
        table = pcsv.read_csv(output)
        assert table.num_rows == 10
        assert table.column_names == ["id", "value", "name"]

    def test_write_csv_with_gzip_compression(self, tmp_path):
        """gzip_compress=True should produce valid gzip CSV."""
        batch = _make_batch(10)
        output = str(tmp_path / "out.csv.gz")

        write_csv(iter([batch]), output, gzip_compress=True)

        # Decompress and read back
        with gzip.open(output, "rb") as f:
            decompressed = f.read()

        table = pcsv.read_csv(io.BytesIO(decompressed))
        assert table.num_rows == 10
        assert table.column_names == ["id", "value", "name"]

    def test_write_csv_preserves_header(self, tmp_path):
        """First line of CSV output should be column headers."""
        batch = _make_batch(5)
        output = str(tmp_path / "header.csv")

        write_csv(iter([batch]), output)

        with open(output, "r") as f:
            first_line = f.readline().strip()
        # PyArrow CSV writer quotes strings; header should contain column names
        assert "id" in first_line
        assert "value" in first_line
        assert "name" in first_line

    def test_write_csv_empty_input(self, tmp_path):
        """No batches should produce a file (empty, no crash)."""
        output = str(tmp_path / "empty.csv")

        write_csv(iter([]), output)

        assert os.path.exists(output)
        assert os.path.getsize(output) == 0

    def test_write_csv_large_data_streams(self, tmp_path):
        """100 batches of 1000 rows each should produce correct row count."""
        batches = [_make_batch(1000) for _ in range(100)]
        output = str(tmp_path / "large.csv")

        write_csv(iter(batches), output)

        table = pcsv.read_csv(output)
        assert table.num_rows == 100000


class TestNfsSafePattern:
    """Tests for NFS-safe atomic write pattern."""

    def test_nfs_pattern_no_fcntl_usage(self):
        """csv_writer source code must not import or use fcntl."""
        source = inspect.getsource(extract_stream)
        source += inspect.getsource(write_csv)
        # Also check module-level imports
        import csvconv.writer.csv_writer as mod
        mod_source = inspect.getsource(mod)
        assert "fcntl" not in mod_source

    def test_nfs_pattern_temp_file_same_directory(self, tmp_path, mocker):
        """Temp file should be created in the same directory as output."""
        calls = []
        original_mkstemp = __import__("tempfile").mkstemp

        def tracking_mkstemp(**kwargs):
            calls.append(kwargs)
            return original_mkstemp(**kwargs)

        mocker.patch("csvconv.writer.csv_writer.tempfile.mkstemp", side_effect=tracking_mkstemp)

        source = io.BytesIO(b"data")
        output = str(tmp_path / "nfs_test.csv")

        extract_stream(source, output)

        assert len(calls) == 1
        assert calls[0].get("dir") == str(tmp_path)
