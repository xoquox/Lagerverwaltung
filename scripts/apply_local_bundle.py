#!/usr/bin/env python3

import sys
from pathlib import Path

from local_bundle import apply_bundle


def main(argv=None):
    argv = list(argv or sys.argv[1:])
    if len(argv) < 1:
        print(f"Usage: {Path(__file__).name} <bundle.zip>", file=sys.stderr)
        return 1
    root_dir = Path(__file__).resolve().parent.parent
    print(str(apply_bundle(root_dir, argv[0])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

