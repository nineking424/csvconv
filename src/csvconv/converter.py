"""High-level conversion orchestration and dispatch."""

import logging
import os

from csvconv.reader.csv_reader import read_streaming
from csvconv.reader.tar_reader import list_csv_members, open_member_stream, extract_member_stream
from csvconv.schema.inference import infer_schema
from csvconv.schema.validation import validate_batch_schema
from csvconv.security import validate_tar_member_path
from csvconv.summary import ConversionSummary
from csvconv.writer.csv_writer import extract_stream
from csvconv.writer.parquet_writer import IncrementalParquetWriter

logger = logging.getLogger("csvconv")


def convert(
    input_path,        # type: str
    output_path,       # type: str
    input_type="csv",  # type: str
    output_type="parquet",  # type: str
    block_size_mb=1,   # type: int
    row_group_size=None,  # type: int
    schema_sample_rows=1000,  # type: int
    gzip=False,        # type: bool
):
    """Top-level dispatch: route to correct reader/writer pipeline.

    Dispatch logic:
      - csv + parquet    -> csv_reader + parquet_writer (streaming RecordBatch)
      - tar.gz + parquet -> tar_reader + csv_reader + parquet_writer (schema inference)
      - tar.gz + csv     -> tar_reader + csv_writer.extract_stream() (raw extraction)
    """
    summary = ConversionSummary()

    if input_type == "csv" and output_type == "parquet":
        _convert_csv_to_parquet(input_path, output_path, block_size_mb, row_group_size, summary)
    elif input_type == "tar.gz" and output_type == "parquet":
        _convert_targz_to_parquet(
            input_path, output_path, block_size_mb, row_group_size,
            schema_sample_rows, summary
        )
    elif input_type == "tar.gz" and output_type == "csv":
        _extract_targz_to_csv(input_path, output_path, gzip, summary)
    else:
        raise ValueError(
            "Unsupported conversion: {} -> {}".format(input_type, output_type)
        )

    return summary


def _convert_csv_to_parquet(input_path, output_path, block_size_mb, row_group_size, summary):
    """Convert a single CSV file to Parquet."""
    file_name = os.path.basename(input_path)
    try:
        # First pass: read a batch to get schema
        batches = list(read_streaming(input_path, block_size_mb=block_size_mb))

        if not batches:
            logger.warning("Empty CSV file: %s", input_path)
            summary.record_success(file_name)
            return

        schema = batches[0].schema

        with IncrementalParquetWriter(output_path, schema, row_group_size=row_group_size) as writer:
            for batch in batches:
                writer.write_batch(batch)

        summary.record_success(file_name)
        logger.info("Converted: %s -> %s", input_path, output_path)

    except Exception as e:
        summary.record_failure(file_name, str(e))
        logger.error("Failed to convert %s: %s", input_path, e)
        raise


def _convert_targz_to_parquet(input_path, output_path, block_size_mb, row_group_size,
                               schema_sample_rows, summary):
    """Convert tar.gz containing CSVs to per-file Parquet.

    For each CSV member in the archive, streams it through csv_reader
    and writes to Parquet using IncrementalParquetWriter. Schema is
    inferred from the first CSV member and enforced on all subsequent files.
    """
    members = list_csv_members(input_path)

    if not members:
        logger.info("No CSV members found in %s", input_path)
        return

    # Infer schema from first CSV member
    schema = infer_schema(input_path, members[0], sample_rows=schema_sample_rows)

    # Create output directory if needed
    os.makedirs(output_path, exist_ok=True)

    for member in members:
        member_basename = os.path.basename(member)
        out_name = os.path.splitext(member_basename)[0] + ".parquet"
        out_file = os.path.join(output_path, out_name)

        try:
            stream = open_member_stream(input_path, member)
            batches = read_streaming(stream, block_size_mb=block_size_mb, schema=schema)

            with IncrementalParquetWriter(out_file, schema, row_group_size=row_group_size) as writer:
                for batch in batches:
                    validate_batch_schema(batch, schema)
                    writer.write_batch(batch)

            summary.record_success(member_basename)
            logger.info("Converted: %s -> %s", member, out_file)

        except Exception as e:
            summary.record_failure(member_basename, str(e))
            logger.error("Failed to convert member %s: %s", member, e)


def _extract_targz_to_csv(input_path, output_path, gzip_compress, summary):
    """Extract CSVs from tar.gz to individual CSV files.

    Raw byte-fidelity extraction using csv_writer.extract_stream().
    Each member is validated for path traversal before extraction.
    """
    members = list_csv_members(input_path)

    # Create output directory if needed
    os.makedirs(output_path, exist_ok=True)

    for member in members:
        member_basename = os.path.basename(member)

        try:
            # Security check: validate path is safe
            validate_tar_member_path(member, output_path)

            # Determine output filename
            out_name = member_basename
            if gzip_compress:
                out_name = out_name + ".gz"
            out_file = os.path.join(output_path, out_name)

            # Extract raw bytes
            stream = extract_member_stream(input_path, member)
            extract_stream(stream, out_file, gzip_compress=gzip_compress)

            summary.record_success(member_basename)
            logger.info("Extracted: %s -> %s", member, out_file)

        except Exception as e:
            summary.record_failure(member_basename, str(e))
            logger.error("Failed to extract member %s: %s", member, e)
