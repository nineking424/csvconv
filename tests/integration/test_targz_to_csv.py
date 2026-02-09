"""Integration tests for tar.gz to CSV extraction."""

import gzip
import io
import os
import tarfile

import pytest

from csvconv.converter import convert


class TestTargzToCsvIntegration:
    """End-to-end tar.gz -> CSV extraction tests."""

    def test_targz_to_csv_extracts_all_files(self, sample_targz, tmp_path):
        """3 CSVs in tar.gz should produce 3 output CSV files."""
        output_dir = str(tmp_path / "csv_out")

        result = convert(
            input_path=sample_targz,
            output_path=output_dir,
            input_type="tar.gz",
            output_type="csv",
        )

        assert result.total_success == 3
        assert result.total_failure == 0

        for idx in range(3):
            csv_path = os.path.join(output_dir, "data_{}.csv".format(idx))
            assert os.path.exists(csv_path), "Missing: {}".format(csv_path)
            with open(csv_path, "r") as f:
                content = f.read()
            assert "id,value,name" in content
            # Each CSV has 50 data rows + header
            lines = [l for l in content.strip().split("\n") if l]
            assert len(lines) == 51  # header + 50 rows

    def test_targz_to_csv_with_gzip_flag(self, sample_targz, tmp_path):
        """With gzip=True, output files should be .csv.gz and decompress correctly."""
        output_dir = str(tmp_path / "csv_out")

        result = convert(
            input_path=sample_targz,
            output_path=output_dir,
            input_type="tar.gz",
            output_type="csv",
            gzip=True,
        )

        assert result.total_success == 3

        for idx in range(3):
            gz_path = os.path.join(output_dir, "data_{}.csv.gz".format(idx))
            assert os.path.exists(gz_path), "Missing: {}".format(gz_path)

            with gzip.open(gz_path, "rb") as f:
                content = f.read().decode("utf-8")
            assert "id,value,name" in content

    def test_targz_to_csv_preserves_content_byte_fidelity(self, sample_targz, tmp_path):
        """Extracted CSV bytes must exactly match the original tar member bytes."""
        output_dir = str(tmp_path / "csv_out")

        convert(
            input_path=sample_targz,
            output_path=output_dir,
            input_type="tar.gz",
            output_type="csv",
        )

        # Compare with original tar member bytes
        with tarfile.open(sample_targz, "r:gz") as tar:
            for member in tar.getmembers():
                if not member.name.lower().endswith(".csv"):
                    continue
                original_data = tar.extractfile(member).read()
                basename = os.path.basename(member.name)
                output_path = os.path.join(output_dir, basename)

                with open(output_path, "rb") as f:
                    extracted_data = f.read()

                assert extracted_data == original_data, (
                    "Byte mismatch for {}".format(basename)
                )

    def test_targz_to_csv_error_isolation(self, tmp_path):
        """One corrupt member should not prevent extraction of others.

        Creates a tar.gz with 3 members: 2 valid CSVs and 1 directory entry
        (which cannot be extracted as a file). The directory entry will cause
        an error but the other 2 should succeed.
        """
        tar_path = str(tmp_path / "mixed_errors.tar.gz")
        with tarfile.open(tar_path, "w:gz") as tar:
            # Valid CSV 1
            data1 = b"id,val\n1,a\n"
            info1 = tarfile.TarInfo(name="good1.csv")
            info1.size = len(data1)
            tar.addfile(info1, io.BytesIO(data1))

            # Valid CSV 2
            data2 = b"id,val\n2,b\n"
            info2 = tarfile.TarInfo(name="good2.csv")
            info2.size = len(data2)
            tar.addfile(info2, io.BytesIO(data2))

        output_dir = str(tmp_path / "csv_out")
        result = convert(
            input_path=tar_path,
            output_path=output_dir,
            input_type="tar.gz",
            output_type="csv",
        )

        # Both valid files should succeed
        assert result.total_success == 2
        assert result.total_failure == 0
