"""``python -m z4j_celery`` - module entry point for the engine doctor."""

from __future__ import annotations

import sys

from z4j_celery.cli import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
