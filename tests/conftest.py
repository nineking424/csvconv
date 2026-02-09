"""Shared test fixtures for csvconv tests.

All test data is generated programmatically - no static fixture files.
"""

import csv
import io
import tarfile

import pytest


@pytest.fixture
def tmp_dir(tmp_path):
    """Provide a temporary directory for test outputs."""
    return tmp_path


@pytest.fixture
def sample_csv(tmp_path):
    """Generate a sample CSV file with 100 rows of int/float/string data."""
    csv_path = tmp_path / "sample.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "value", "name"])
        for i in range(100):
            writer.writerow([i, float(i) * 1.5, "name_{}".format(i)])
    return str(csv_path)


@pytest.fixture
def large_csv(tmp_path):
    """Generate a CSV file larger than 1MB for block_size testing."""
    csv_path = tmp_path / "large.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "value", "description"])
        for i in range(50000):
            writer.writerow([i, float(i) * 0.1, "x" * 50])
    return str(csv_path)


@pytest.fixture
def empty_csv(tmp_path):
    """Generate a CSV file with only a header row."""
    csv_path = tmp_path / "empty.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "value", "name"])
    return str(csv_path)


@pytest.fixture
def sample_targz(tmp_path):
    """Generate a tar.gz archive containing 3 CSV files with the same schema."""
    tar_path = tmp_path / "sample.tar.gz"
    with tarfile.open(str(tar_path), "w:gz") as tar:
        for idx in range(3):
            csv_content = "id,value,name\n"
            for i in range(50):
                csv_content += "{},{},{}\n".format(idx * 50 + i, float(idx * 50 + i) * 1.5, "name_{}".format(i))
            data = csv_content.encode("utf-8")
            info = tarfile.TarInfo(name="data_{}.csv".format(idx))
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return str(tar_path)


@pytest.fixture
def schema_mismatch_targz(tmp_path):
    """Generate a tar.gz where the 2nd CSV has a different schema."""
    tar_path = tmp_path / "mismatch.tar.gz"
    with tarfile.open(str(tar_path), "w:gz") as tar:
        # First CSV: id, value, name
        csv1 = "id,value,name\n1,1.0,a\n2,2.0,b\n"
        data1 = csv1.encode("utf-8")
        info1 = tarfile.TarInfo(name="file1.csv")
        info1.size = len(data1)
        tar.addfile(info1, io.BytesIO(data1))

        # Second CSV: different schema (id, category, count)
        csv2 = "id,category,count\n1,x,10\n2,y,20\n"
        data2 = csv2.encode("utf-8")
        info2 = tarfile.TarInfo(name="file2.csv")
        info2.size = len(data2)
        tar.addfile(info2, io.BytesIO(data2))

        # Third CSV: same as first
        csv3 = "id,value,name\n3,3.0,c\n4,4.0,d\n"
        data3 = csv3.encode("utf-8")
        info3 = tarfile.TarInfo(name="file3.csv")
        info3.size = len(data3)
        tar.addfile(info3, io.BytesIO(data3))
    return str(tar_path)


@pytest.fixture
def mixed_targz(tmp_path):
    """Generate a tar.gz containing CSV and non-CSV files."""
    tar_path = tmp_path / "mixed.tar.gz"
    with tarfile.open(str(tar_path), "w:gz") as tar:
        for name, content in [
            ("data1.csv", "id,val\n1,a\n"),
            ("readme.txt", "This is not a CSV"),
            ("data2.csv", "id,val\n2,b\n"),
            ("nested/data3.csv", "id,val\n3,c\n"),
        ]:
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return str(tar_path)


@pytest.fixture
def malicious_targz(tmp_path):
    """Generate a tar.gz with path traversal attempts."""
    tar_path = tmp_path / "malicious.tar.gz"
    with tarfile.open(str(tar_path), "w:gz") as tar:
        for name in [
            "../../etc/passwd.csv",
            "/etc/shadow.csv",
            "data/../../../tmp/evil.csv",
            "normal.csv",
        ]:
            data = b"id,val\n1,a\n"
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return str(tar_path)


@pytest.fixture
def empty_targz(tmp_path):
    """Generate a tar.gz with no CSV files (only a .txt file)."""
    tar_path = tmp_path / "empty.tar.gz"
    with tarfile.open(str(tar_path), "w:gz") as tar:
        data = b"not a csv"
        info = tarfile.TarInfo(name="readme.txt")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    return str(tar_path)
