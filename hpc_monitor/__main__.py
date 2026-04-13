"""Allow `python3 -m hpc_monitor` to invoke the CLI."""

import sys

from .cli import main

sys.exit(main())
