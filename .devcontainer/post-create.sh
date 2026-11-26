#!/usr/bin/env bash
# One-time setup when the Codespace is created.
set -euo pipefail

pip install --user --quiet uv
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh

# Pre-download PySpark + Delta so the first test run is fast
uv sync --python 3.12

java -version 2>&1 | head -1
uv --version
databricks --version
