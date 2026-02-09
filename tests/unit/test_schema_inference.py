"""Unit tests for schema inference."""

import pyarrow as pa
import pytest

from csvconv.schema.inference import infer_schema


class TestInferSchema:
    """Tests for schema inference from tar.gz CSV members."""

    def test_returns_pyarrow_schema(self, sample_targz):
        members = ["data_0.csv"]  # known member from fixture
        schema = infer_schema(sample_targz, members[0])
        assert isinstance(schema, pa.Schema)

    def test_schema_has_correct_fields(self, sample_targz):
        schema = infer_schema(sample_targz, "data_0.csv")
        assert "id" in schema.names
        assert "value" in schema.names
        assert "name" in schema.names

    def test_respects_sample_rows(self, sample_targz):
        schema = infer_schema(sample_targz, "data_0.csv", sample_rows=5)
        # Should still produce a valid schema
        assert isinstance(schema, pa.Schema)
        assert len(schema.names) == 3

    def test_handles_all_string_columns(self, tmp_path):
        """CSV with all-string data should produce string-type schema."""
        import io
        import tarfile

        tar_path = str(tmp_path / "strings.tar.gz")
        with tarfile.open(tar_path, "w:gz") as tar:
            content = "a,b,c\nfoo,bar,baz\nhello,world,test\n"
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name="strings.csv")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        schema = infer_schema(tar_path, "strings.csv")
        for field in schema:
            assert field.type in (pa.string(), pa.large_string())
