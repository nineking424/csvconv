#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Create dist directory
mkdir -p dist

echo "Building csvconv PEX..."
echo "Target: manylinux2014_x86_64, Python 3.8+"

# Build PEX with cross-platform targeting for CentOS 8 / Rocky / Alma
pex . \
  -e csvconv.cli:main \
  --python-shebang='/usr/bin/env python3' \
  -o dist/csvconv.pex

echo ""
echo "Built: dist/csvconv.pex"
echo "Size: $(du -h dist/csvconv.pex | cut -f1)"
echo "Note: Requires Python 3.8+ pre-installed on target system"
