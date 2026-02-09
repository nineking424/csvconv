"""Unit tests for csvconv security module."""

import os
import pytest

from csvconv.security import validate_tar_member_path, validate_input_path
from csvconv.errors import SecurityError, InputValidationError


class TestValidateTarMemberPath:
    """Tests for path traversal prevention."""

    def test_rejects_absolute_path(self, tmp_path):
        with pytest.raises(SecurityError):
            validate_tar_member_path("/etc/passwd.csv", str(tmp_path))

    def test_rejects_traversal(self, tmp_path):
        with pytest.raises(SecurityError):
            validate_tar_member_path("../../etc/passwd.csv", str(tmp_path))

    def test_rejects_dot_dot_in_middle(self, tmp_path):
        with pytest.raises(SecurityError):
            validate_tar_member_path("data/../../../etc/passwd.csv", str(tmp_path))

    def test_accepts_normal_path(self, tmp_path):
        result = validate_tar_member_path("data/file.csv", str(tmp_path))
        expected = os.path.join(str(tmp_path), "data", "file.csv")
        assert os.path.normpath(result) == os.path.normpath(expected)

    def test_accepts_simple_filename(self, tmp_path):
        result = validate_tar_member_path("file.csv", str(tmp_path))
        assert result == os.path.join(str(tmp_path), "file.csv")

    def test_rejects_backslash_traversal(self, tmp_path):
        """Windows-style path traversal should also be rejected."""
        with pytest.raises(SecurityError):
            validate_tar_member_path("..\\..\\etc\\passwd.csv", str(tmp_path))


class TestValidateInputPath:
    """Tests for input file validation."""

    def test_rejects_nonexistent(self):
        with pytest.raises(InputValidationError):
            validate_input_path("/nonexistent/file.csv")

    def test_rejects_wrong_extension(self, tmp_path):
        bad_file = tmp_path / "file.json"
        bad_file.write_text("not csv")
        with pytest.raises(InputValidationError):
            validate_input_path(str(bad_file))

    def test_accepts_csv_extension(self, sample_csv):
        # Should not raise
        validate_input_path(sample_csv)

    def test_accepts_targz_extension(self, sample_targz):
        # Should not raise
        validate_input_path(sample_targz)
