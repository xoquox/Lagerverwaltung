#!/usr/bin/env python3

from pathlib import Path

from local_bundle import create_bundle


def main():
    root_dir = Path(__file__).resolve().parent.parent
    print(str(create_bundle(root_dir)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

