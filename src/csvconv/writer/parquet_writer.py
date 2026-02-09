"""Incremental Parquet writer using PyArrow."""

import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


class IncrementalParquetWriter:
    """Write RecordBatches incrementally to a Parquet file.

    Uses row_group_size to control flush frequency.
    Implements NFS-safe atomic write pattern: write to temp file,
    fsync, then os.replace.
    """

    def __init__(self, output_path, schema, row_group_size=None):
        # type: (str, pa.Schema, int) -> None
        self._output_path = output_path
        self._schema = schema
        self._row_group_size = row_group_size

        # Validate output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.isdir(output_dir):
            raise FileNotFoundError(
                "Output directory does not exist: {}".format(output_dir)
            )

        # Write to temp file for NFS-safe atomic write
        self._output_dir = output_dir or "."
        self._tmp_fd, self._tmp_path = tempfile.mkstemp(
            dir=self._output_dir, suffix=".parquet.tmp"
        )
        os.close(self._tmp_fd)

        writer_kwargs = {}
        if row_group_size is not None:
            writer_kwargs["write_batch_size"] = row_group_size  # PyArrow 16.x parameter name

        try:
            self._writer = pq.ParquetWriter(self._tmp_path, schema, **writer_kwargs)
        except Exception:
            # Clean up temp file on failure
            if os.path.exists(self._tmp_path):
                os.unlink(self._tmp_path)
            raise

        self._closed = False

    def write_batch(self, batch):
        # type: (pa.RecordBatch) -> None
        """Write a RecordBatch to the Parquet file."""
        if self._closed:
            raise RuntimeError("Writer is already closed")
        table = pa.Table.from_batches([batch])
        self._writer.write_table(table)

    def close(self):
        # type: () -> None
        """Close the writer and atomically move to final path."""
        if self._closed:
            return
        self._writer.close()
        self._closed = True

        # fsync for NFS safety
        fd = os.open(self._tmp_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

        # Atomic rename
        os.replace(self._tmp_path, self._output_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # On error, clean up temp file
            self._writer.close()
            self._closed = True
            if os.path.exists(self._tmp_path):
                os.unlink(self._tmp_path)
        else:
            self.close()
        return False
