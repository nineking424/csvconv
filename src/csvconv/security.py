"""Security utilities for path traversal prevention and input validation."""

import os

from csvconv.errors import InputValidationError, SecurityError

ALLOWED_INPUT_EXTENSIONS = {".csv", ".tar.gz", ".tgz"}


def validate_tar_member_path(member_name, extract_dir):
    # type: (str, str) -> str
    """Prevent path traversal in tar member names.

    Returns the safe absolute path within extract_dir,
    or raises SecurityError if the path attempts traversal.
    """
    # Normalize the member name
    member_name = member_name.replace("\\", "/")

    # Reject absolute paths
    if member_name.startswith("/"):
        raise SecurityError(
            "Absolute path in tar member rejected: {}".format(member_name)
        )

    # Reject any path component that is ".."
    parts = member_name.split("/")
    if ".." in parts:
        raise SecurityError(
            "Path traversal detected in tar member: {}".format(member_name)
        )

    # Resolve the full path and verify it's within extract_dir
    safe_path = os.path.normpath(os.path.join(extract_dir, member_name))
    extract_dir_real = os.path.realpath(extract_dir)
    safe_path_real = os.path.realpath(safe_path)

    if not safe_path_real.startswith(extract_dir_real + os.sep) and safe_path_real != extract_dir_real:
        raise SecurityError(
            "Path escapes extract directory: {} -> {}".format(member_name, safe_path_real)
        )

    return safe_path


def validate_input_path(path):
    # type: (str) -> None
    """Validate that input file exists and has an allowed extension.

    Raises InputValidationError if validation fails.
    """
    if not os.path.exists(path):
        raise InputValidationError(
            "Input file does not exist: {}".format(path)
        )

    if not os.path.isfile(path):
        raise InputValidationError(
            "Input path is not a file: {}".format(path)
        )

    # Check extension
    lower_path = path.lower()
    has_valid_ext = False
    for ext in ALLOWED_INPUT_EXTENSIONS:
        if lower_path.endswith(ext):
            has_valid_ext = True
            break

    if not has_valid_ext:
        raise InputValidationError(
            "Unsupported input file type: {}. Allowed: {}".format(
                path, ", ".join(sorted(ALLOWED_INPUT_EXTENSIONS))
            )
        )
