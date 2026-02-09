"""Schema validation and mismatch detection."""

from csvconv.errors import SchemaMismatchError


def validate_batch_schema(batch, expected):
    # type: (pyarrow.RecordBatch, pyarrow.Schema) -> None
    """Validate that a RecordBatch schema matches the expected schema.

    Checks column count, column names, and column types.
    Raises SchemaMismatchError with a descriptive message if any mismatch.
    """
    actual = batch.schema

    # Check column count
    if len(actual) != len(expected):
        raise SchemaMismatchError(
            "Schema column count mismatch: expected {} columns, got {}".format(
                len(expected), len(actual)
            ),
            expected=expected,
            actual=actual,
        )

    # Check column names
    for i, (exp_field, act_field) in enumerate(zip(expected, actual)):
        if exp_field.name != act_field.name:
            raise SchemaMismatchError(
                "Schema column name mismatch at position {}: expected '{}', got '{}'".format(
                    i, exp_field.name, act_field.name
                ),
                expected=expected,
                actual=actual,
            )

    # Check column types
    for i, (exp_field, act_field) in enumerate(zip(expected, actual)):
        if exp_field.type != act_field.type:
            raise SchemaMismatchError(
                "Schema type mismatch for column '{}': expected {}, got {}".format(
                    exp_field.name, exp_field.type, act_field.type
                ),
                expected=expected,
                actual=actual,
            )
