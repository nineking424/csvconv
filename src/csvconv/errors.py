"""Custom exception hierarchy for csvconv."""


class CsvconvError(Exception):
    """Base exception for all csvconv errors."""

    pass


class InputError(CsvconvError):
    """Error related to input file reading or validation."""

    pass


class OutputError(CsvconvError):
    """Error related to output file writing."""

    pass


class SchemaMismatchError(CsvconvError):
    """Error when CSV schema does not match expected schema."""

    def __init__(self, message, file_name=None, expected=None, actual=None):
        self.file_name = file_name
        self.expected = expected
        self.actual = actual
        super().__init__(message)


class SecurityError(CsvconvError):
    """Error related to security violations (e.g., path traversal)."""

    pass


class InputValidationError(InputError):
    """Error when input file fails validation checks."""

    pass


class MemberNotFoundError(InputError):
    """Error when a tar.gz member is not found."""

    pass
