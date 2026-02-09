"""Entry point for python -m csvconv."""

import sys


def _main():
    try:
        from csvconv.cli import main

        sys.exit(main())
    except ImportError:
        print("csvconv CLI not yet implemented", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    _main()
