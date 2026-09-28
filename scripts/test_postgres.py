"""Run unit and integration tests using the configured PostgreSQL service."""
from pathlib import Path
import subprocess
import sys


def main():
    for suite in ('tests', 'tests/postgres'):
        result = subprocess.run(
            [sys.executable, '-m', 'unittest', 'discover', '-s', suite, '-v'],
            cwd=Path(__file__).resolve().parents[1],
        )
        if result.returncode:
            return result.returncode
    return 0


if __name__ == '__main__':
    sys.exit(main())
