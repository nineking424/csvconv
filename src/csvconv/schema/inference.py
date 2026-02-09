"""Schema inference from CSV sample rows."""

import pyarrow.csv as pcsv

from csvconv.reader.tar_reader import open_member_stream


def infer_schema(tar_path, member_name, sample_rows=1000):
    # type: (str, str, int) -> pyarrow.Schema
    """Infer PyArrow schema from a CSV file within a tar.gz archive.

    Reads the first sample_rows rows from the specified member
    to determine column types.

    Args:
        tar_path: Path to the tar.gz archive.
        member_name: Name of the CSV member to use for inference.
        sample_rows: Number of rows to sample for type inference.

    Returns:
        PyArrow Schema with inferred column types.
    """
    stream = open_member_stream(tar_path, member_name)

    read_options = pcsv.ReadOptions(block_size=1024 * 1024)
    reader = pcsv.open_csv(stream, read_options=read_options)

    rows_read = 0
    batches = []
    for batch in reader:
        batches.append(batch)
        rows_read += batch.num_rows
        if rows_read >= sample_rows:
            break

    if not batches:
        # Fallback: read just the header
        stream.seek(0)
        table = pcsv.read_csv(stream)
        return table.schema

    return batches[0].schema
