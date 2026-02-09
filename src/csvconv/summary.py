"""Conversion summary tracking and reporting."""


class ConversionSummary:
    """Track success and failure counts for file conversions."""

    def __init__(self):
        self._successes = []  # type: list
        self._failures = []   # type: list

    def record_success(self, file_name):
        # type: (str) -> None
        """Record a successful file conversion."""
        self._successes.append(file_name)

    def record_failure(self, file_name, reason):
        # type: (str, str) -> None
        """Record a failed file conversion with reason."""
        self._failures.append({"file": file_name, "reason": reason})

    @property
    def total_success(self):
        # type: () -> int
        return len(self._successes)

    @property
    def total_failure(self):
        # type: () -> int
        return len(self._failures)

    @property
    def successes(self):
        # type: () -> list
        return list(self._successes)

    @property
    def failures(self):
        # type: () -> list
        return list(self._failures)

    def get_report(self):
        # type: () -> str
        """Generate a formatted summary report."""
        lines = []
        lines.append("Conversion Summary")
        lines.append("=" * 40)
        lines.append("Success: {} file(s)".format(self.total_success))
        lines.append("Failure: {} file(s)".format(self.total_failure))

        if self._successes:
            lines.append("")
            lines.append("Successful files:")
            for f in self._successes:
                lines.append("  - {}".format(f))

        if self._failures:
            lines.append("")
            lines.append("Failed files:")
            for f in self._failures:
                lines.append("  - {}: {}".format(f["file"], f["reason"]))

        return "\n".join(lines)
