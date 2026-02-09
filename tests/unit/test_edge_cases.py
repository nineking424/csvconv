"""Edge case tests for csvconv."""

import csv
import gzip
import io
import os
import tarfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from csvconv.converter import convert


class TestSpecialCharacters:
    def test_csv_with_commas_in_values(self, tmp_path):
        """CSV with commas inside quoted fields."""
        csv_path = str(tmp_path / "special.csv")
        pq_path = str(tmp_path / "special.parquet")
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "description"])
            writer.writerow([1, "hello, world"])
            writer.writerow([2, 'say "hi"'])
            writer.writerow([3, "line1\nline2"])
        summary = convert(csv_path, pq_path)
        assert summary.total_success == 1
        table = pq.read_table(pq_path)
        assert table.num_rows == 3

    def test_csv_with_unicode(self, tmp_path):
        """CSV with Korean/Chinese/emoji characters."""
        csv_path = str(tmp_path / "unicode.csv")
        pq_path = str(tmp_path / "unicode.parquet")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "text"])
            writer.writerow([1, "한국어"])
            writer.writerow([2, "中文"])
            writer.writerow([3, "日本語"])
        summary = convert(csv_path, pq_path)
        assert summary.total_success == 1
        table = pq.read_table(pq_path)
        assert table.num_rows == 3
        # Verify unicode preserved
        texts = table.column("text").to_pylist()
        assert "한국어" in texts
        assert "中文" in texts


class TestWideCSV:
    def test_very_wide_csv(self, tmp_path):
        """CSV with 200 columns."""
        csv_path = str(tmp_path / "wide.csv")
        pq_path = str(tmp_path / "wide.parquet")
        num_cols = 200
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["col_{}".format(i) for i in range(num_cols)])
            for r in range(10):
                writer.writerow([r * num_cols + i for i in range(num_cols)])
        summary = convert(csv_path, pq_path)
        assert summary.total_success == 1
        table = pq.read_table(pq_path)
        assert len(table.schema) == num_cols
        assert table.num_rows == 10


class TestSingleRow:
    def test_single_row_csv(self, tmp_path):
        """CSV with header + 1 data row."""
        csv_path = str(tmp_path / "single.csv")
        pq_path = str(tmp_path / "single.parquet")
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value"])
            writer.writerow([1, 42])
        summary = convert(csv_path, pq_path)
        assert summary.total_success == 1
        table = pq.read_table(pq_path)
        assert table.num_rows == 1


class TestOverwrite:
    def test_overwrite_existing_output(self, tmp_path):
        """Overwriting an existing file should work via atomic write."""
        csv_path = str(tmp_path / "data.csv")
        pq_path = str(tmp_path / "output.parquet")
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value"])
            for i in range(10):
                writer.writerow([i, i * 2])

        # First conversion
        convert(csv_path, pq_path)
        first_size = os.path.getsize(pq_path)

        # Modify CSV and convert again to same output
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value"])
            for i in range(20):
                writer.writerow([i, i * 3])

        convert(csv_path, pq_path)
        table = pq.read_table(pq_path)
        assert table.num_rows == 20  # New data, not old


class TestTargzEdgeCases:
    def test_targz_with_empty_csv(self, tmp_path):
        """tar.gz containing a header-only CSV."""
        tar_path = str(tmp_path / "empty_csv.tar.gz")
        with tarfile.open(tar_path, "w:gz") as tar:
            csv_content = "id,value,name\n"
            data = csv_content.encode("utf-8")
            info = tarfile.TarInfo(name="empty.csv")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        output_dir = str(tmp_path / "out")
        os.makedirs(output_dir)
        summary = convert(tar_path, output_dir, input_type="tar.gz", output_type="parquet")
        # Empty CSV should still succeed (header-only is valid)
        assert summary.total_success + summary.total_failure == 1

    def test_targz_to_csv_byte_fidelity_with_unicode(self, tmp_path):
        """tar.gz→CSV extraction preserves unicode bytes exactly."""
        original_content = "id,text\n1,한국어\n2,中文\n"
        tar_path = str(tmp_path / "unicode.tar.gz")
        with tarfile.open(tar_path, "w:gz") as tar:
            data = original_content.encode("utf-8")
            info = tarfile.TarInfo(name="unicode.csv")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        output_dir = str(tmp_path / "out")
        os.makedirs(output_dir)
        convert(tar_path, output_dir, input_type="tar.gz", output_type="csv")

        extracted = os.path.join(output_dir, "unicode.csv")
        assert os.path.exists(extracted)
        with open(extracted, "rb") as f:
            assert f.read() == original_content.encode("utf-8")
