"""Unit tests for schema validation."""

import pyarrow as pa
import pytest

from csvconv.schema.validation import validate_batch_schema
from csvconv.errors import SchemaMismatchError


@pytest.fixture
def expected_schema():
    return pa.schema([
        ("id", pa.int64()),
        ("value", pa.float64()),
        ("name", pa.string()),
    ])


class TestValidateBatchSchema:
    """Tests for schema validation."""

    def test_matching_schema_passes(self, expected_schema):
        batch = pa.RecordBatch.from_pydict(
            {"id": [1, 2], "value": [1.0, 2.0], "name": ["a", "b"]},
            schema=expected_schema,
        )
        # Should not raise
        validate_batch_schema(batch, expected_schema)

    def test_mismatched_column_count(self, expected_schema):
        batch = pa.RecordBatch.from_pydict(
            {"id": [1, 2], "value": [1.0, 2.0]},
        )
        with pytest.raises(SchemaMismatchError, match="column count"):
            validate_batch_schema(batch, expected_schema)

    def test_mismatched_column_name(self, expected_schema):
        wrong_schema = pa.schema([
            ("id", pa.int64()),
            ("amount", pa.float64()),
            ("name", pa.string()),
        ])
        batch = pa.RecordBatch.from_pydict(
            {"id": [1], "amount": [1.0], "name": ["a"]},
            schema=wrong_schema,
        )
        with pytest.raises(SchemaMismatchError, match="column name"):
            validate_batch_schema(batch, expected_schema)

    def test_mismatched_column_type(self, expected_schema):
        wrong_schema = pa.schema([
            ("id", pa.string()),  # should be int64
            ("value", pa.float64()),
            ("name", pa.string()),
        ])
        batch = pa.RecordBatch.from_pydict(
            {"id": ["1"], "value": [1.0], "name": ["a"]},
            schema=wrong_schema,
        )
        with pytest.raises(SchemaMismatchError, match="type mismatch"):
            validate_batch_schema(batch, expected_schema)
