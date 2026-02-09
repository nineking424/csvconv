"""tar.gz archive reader with security controls."""

import io
import tarfile

from csvconv.errors import MemberNotFoundError


def list_csv_members(tar_path):
    # type: (str) -> list
    """List CSV file members inside a tar.gz archive.

    Returns a sorted list of member names ending in .csv.
    """
    members = []
    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar.getmembers():
            if member.isfile() and member.name.lower().endswith(".csv"):
                members.append(member.name)
    return sorted(members)


def open_member_stream(tar_path, member_name):
    # type: (str, str) -> io.BytesIO
    """Open a tar member and return its content as a BytesIO stream.

    Args:
        tar_path: Path to the tar.gz archive.
        member_name: Name of the member to extract.

    Returns:
        BytesIO object containing the member's data.

    Raises:
        MemberNotFoundError: If the member doesn't exist in the archive.
    """
    with tarfile.open(tar_path, "r:gz") as tar:
        try:
            member = tar.getmember(member_name)
        except KeyError:
            raise MemberNotFoundError(
                "Member not found in archive: {}".format(member_name)
            )

        f = tar.extractfile(member)
        if f is None:
            raise MemberNotFoundError(
                "Cannot extract member (not a regular file): {}".format(member_name)
            )

        # Read into BytesIO so we don't hold the tarfile open
        data = f.read()
        return io.BytesIO(data)


def extract_member_stream(tar_path, member_name):
    # type: (str, str) -> io.BytesIO
    """Open a raw binary stream for a tar member (no parsing).

    Same as open_member_stream but semantically indicates raw extraction usage.
    """
    return open_member_stream(tar_path, member_name)
