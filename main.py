from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ruyi_agent.entrypoints.main import (
    app,
    main,
    run_gateway,
)


if __name__ == "__main__":
    main()
