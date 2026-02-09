"""Streaming CSV reader using PyArrow."""


import pyarrow.csv as pcsv


def read_streaming(
    source,  # type: Union[str, BinaryIO]
    block_size_mb=1,  # type: int
    schema=None,  # type: Optional[pa.Schema]
):  # type: (...) -> Iterator[pa.RecordBatch]
    """Yield RecordBatch chunks from CSV using PyArrow streaming API.

    Args:
        source: File path (str) or binary file-like object.
        block_size_mb: Block size in megabytes. Internally converted to bytes
                       via block_size_mb * 1024 * 1024 for PyArrow's
                       ReadOptions(block_size=...) which expects bytes.
        schema: Optional PyArrow schema to enforce on all batches.

    Yields:
        pa.RecordBatch for each chunk read from the CSV.
    """
    block_size_bytes = block_size_mb * 1024 * 1024

    read_options = pcsv.ReadOptions(block_size=block_size_bytes)

    convert_options = None
    if schema is not None:
        convert_options = pcsv.ConvertOptions(column_types=schema)

    reader = pcsv.open_csv(source, read_options=read_options, convert_options=convert_options)

    for batch in reader:
        if batch.num_rows > 0:
            yield batch
