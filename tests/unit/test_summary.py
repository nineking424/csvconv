"""Unit tests for conversion summary."""

from csvconv.summary import ConversionSummary


class TestConversionSummary:
    """Tests for summary tracking."""

    def test_tracks_success(self):
        summary = ConversionSummary()
        summary.record_success("file1.csv")
        assert summary.total_success == 1
        assert summary.total_failure == 0

    def test_tracks_failure_with_reason(self):
        summary = ConversionSummary()
        summary.record_failure("file2.csv", "Schema mismatch")
        assert summary.total_success == 0
        assert summary.total_failure == 1
        assert summary.failures[0]["reason"] == "Schema mismatch"

    def test_report_format(self):
        summary = ConversionSummary()
        summary.record_success("a.csv")
        summary.record_success("b.csv")
        summary.record_success("c.csv")
        summary.record_failure("d.csv", "Error")
        report = summary.get_report()
        assert "Success: 3" in report
        assert "Failure: 1" in report
        assert "d.csv" in report
        assert "Error" in report

    def test_empty_summary(self):
        summary = ConversionSummary()
        assert summary.total_success == 0
        assert summary.total_failure == 0
        report = summary.get_report()
        assert "Success: 0" in report
