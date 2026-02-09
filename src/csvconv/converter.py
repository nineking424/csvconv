"""High-level conversion orchestration and dispatch."""

import logging
import os

from csvconv.reader.csv_reader import read_streaming
from csvconv.summary import ConversionSummary
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
    """Convert tar.gz containing CSVs to per-file Parquet. Placeholder for Sprint 2."""
    raise NotImplementedError("tar.gz -> parquet conversion not yet implemented")


def _extract_targz_to_csv(input_path, output_path, gzip_compress, summary):
    """Extract CSVs from tar.gz to individual CSV files. Placeholder for Sprint 3."""
    raise NotImplementedError("tar.gz -> csv extraction not yet implemented")
