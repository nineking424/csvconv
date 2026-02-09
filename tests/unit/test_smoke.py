"""Smoke test: verify csvconv package imports correctly."""


def test_import_csvconv():
    """Test that csvconv can be imported."""
    import csvconv

    assert hasattr(csvconv, "__version__")


def test_version_is_string():
    """Test that version is a string."""
    import csvconv

    assert isinstance(csvconv.__version__, str)


def test_version_format():
    """Test that version follows semver format."""
    import csvconv

    parts = csvconv.__version__.split(".")
    assert len(parts) == 3
    for part in parts:
        assert part.isdigit()
