#!/usr/bin/env python3
"""Validate generated .ipynb notebooks before deployment.

Checks:
  1. Valid JSON (parseable by json.load)
  2. No remaining {{placeholder}} strings
  3. Notebook structure: top-level 'cells' and 'metadata' keys;
     every cell has 'cell_type' and 'source'; every code cell has 'outputs'

Usage:
    python validate_notebooks.py <directory>

Validates all .ipynb files in <directory>. Exits with code 0 if all pass,
or code 1 if any check fails.
"""

import json
import re
import sys
from pathlib import Path

PLACEHOLDER_RE = re.compile(r"\{\{[^}]+\}\}")


def validate_notebook(path: Path) -> list[str]:
    errors = []

    # 1. Valid JSON
    try:
        with open(path, encoding="utf-8") as f:
            nb = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return [f"Invalid JSON: {e}"]

    # 2. No remaining placeholders
    raw = path.read_text(encoding="utf-8")
    remaining = PLACEHOLDER_RE.findall(raw)
    if remaining:
        unique = sorted(set(remaining))
        errors.append(f"Unresolved placeholders: {unique}")

    # 3. Notebook structure
    if "cells" not in nb:
        errors.append("Missing top-level 'cells' key")
    if "metadata" not in nb:
        errors.append("Missing top-level 'metadata' key")

    for i, cell in enumerate(nb.get("cells", [])):
        prefix = f"Cell {i}"
        if "cell_type" not in cell:
            errors.append(f"{prefix}: missing 'cell_type'")
        if "source" not in cell:
            errors.append(f"{prefix}: missing 'source'")
        if cell.get("cell_type") == "code" and "outputs" not in cell:
            errors.append(f"{prefix}: code cell missing 'outputs'")

    return errors


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <directory>", file=sys.stderr)
        return 2

    directory = Path(sys.argv[1])
    if not directory.is_dir():
        print(f"Error: {directory} is not a directory", file=sys.stderr)
        return 2

    notebooks = sorted(directory.glob("*.ipynb"))
    if not notebooks:
        print(f"No .ipynb files found in {directory}")
        return 2

    all_passed = True
    for nb_path in notebooks:
        errors = validate_notebook(nb_path)
        if errors:
            all_passed = False
            print(f"FAIL: {nb_path.name}")
            for err in errors:
                print(f"  - {err}")
        else:
            print(f"OK:   {nb_path.name}")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
