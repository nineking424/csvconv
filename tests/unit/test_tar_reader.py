"""Unit tests for csvconv tar.gz reader."""

import pytest

from csvconv.reader.tar_reader import list_csv_members, open_member_stream
from csvconv.errors import MemberNotFoundError


class TestListCsvMembers:
    """Tests for listing CSV members in tar.gz."""

    def test_returns_csv_files_only(self, mixed_targz):
        members = list_csv_members(mixed_targz)
        assert len(members) == 3
        assert all(m.endswith(".csv") for m in members)

    def test_handles_nested_directories(self, mixed_targz):
        members = list_csv_members(mixed_targz)
        nested = [m for m in members if "/" in m]
        assert len(nested) >= 1

    def test_empty_archive(self, empty_targz):
        members = list_csv_members(empty_targz)
        assert len(members) == 0

    def test_returns_sorted_list(self, sample_targz):
        members = list_csv_members(sample_targz)
        assert members == sorted(members)


class TestOpenMemberStream:
    """Tests for opening tar member streams."""

    def test_returns_readable_stream(self, sample_targz):
        members = list_csv_members(sample_targz)
        stream = open_member_stream(sample_targz, members[0])
        data = stream.read()
        assert len(data) > 0
        assert b"id,value,name" in data

    def test_invalid_member_raises(self, sample_targz):
        with pytest.raises(MemberNotFoundError):
            open_member_stream(sample_targz, "nonexistent.csv")

    def test_reads_correct_content(self, sample_targz):
        members = list_csv_members(sample_targz)
        for member in members:
            stream = open_member_stream(sample_targz, member)
            content = stream.read().decode("utf-8")
            lines = content.strip().split("\n")
            assert lines[0] == "id,value,name"
            assert len(lines) == 51  # header + 50 rows
