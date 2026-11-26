#!/usr/bin/env bash
# One-time setup when the Codespace is created.
set -euo pipefail

# Install to /usr/local/bin: `pip install --user` lands in ~/.local/bin, which is not on PATH here
curl -LsSf https://astral.sh/uv/install.sh | sudo env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh

# Pre-download PySpark + Delta so the first test run is fast
uv sync --python 3.12

java -version 2>&1 | head -1
uv --version
databricks --version
