"""CSV writer with NFS-safe atomic write pattern."""

import gzip
import io
import os
import tempfile

import pyarrow as pa
import pyarrow.csv as pcsv


_CHUNK_SIZE = 64 * 1024  # 64KB


def extract_stream(source, output_path, gzip_compress=False):
    # type: (io.IOBase, str, bool) -> None
    """Raw byte-fidelity extraction from a binary stream to a file.

    Uses NFS-safe atomic write pattern: temp file -> fsync -> os.replace().
    Chunked copy (64KB) keeps memory bounded.

    Args:
        source: A binary stream (BytesIO or file-like object).
        output_path: Destination file path.
        gzip_compress: If True, wrap output in gzip compression.
    """
    output_dir = os.path.dirname(output_path) or "."
    fd, tmp_path = tempfile.mkstemp(dir=output_dir, suffix=".csv.tmp")
    os.close(fd)

    try:
        if gzip_compress:
            with gzip.open(tmp_path, "wb") as gz_out:
                while True:
                    chunk = source.read(_CHUNK_SIZE)
                    if not chunk:
                        break
                    gz_out.write(chunk)
        else:
            with open(tmp_path, "wb") as f_out:
                while True:
                    chunk = source.read(_CHUNK_SIZE)
                    if not chunk:
                        break
                    f_out.write(chunk)

        # fsync for NFS safety
        fd = os.open(tmp_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

        # Atomic rename
        os.replace(tmp_path, output_path)

    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def write_csv(batches, output_path, gzip_compress=False):
    # type: (object, str, bool) -> None
    """Write an iterator of PyArrow RecordBatches as CSV.

    Uses NFS-safe atomic write pattern: temp file -> fsync -> os.replace().

    Args:
        batches: Iterator of pyarrow.RecordBatch objects.
        output_path: Destination file path.
        gzip_compress: If True, gzip compress the output.
    """
    output_dir = os.path.dirname(output_path) or "."
    fd, tmp_path = tempfile.mkstemp(dir=output_dir, suffix=".csv.tmp")
    os.close(fd)

    try:
        header_written = False

        if gzip_compress:
            out_file = gzip.open(tmp_path, "wb")
        else:
            out_file = open(tmp_path, "wb")

        try:
            for batch in batches:
                # Convert batch to CSV bytes via a buffer
                buf = io.BytesIO()
                write_options = pcsv.WriteOptions(
                    include_header=not header_written
                )
                pcsv.write_csv(
                    pa.Table.from_batches([batch]),
                    buf,
                    write_options=write_options,
                )
                buf.seek(0)
                out_file.write(buf.read())
                header_written = True
        finally:
            out_file.close()

        # fsync for NFS safety
        fd = os.open(tmp_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

        # Atomic rename
        os.replace(tmp_path, output_path)

    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise
