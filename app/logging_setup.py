from __future__ import annotations

import logging
import sys
from typing import Optional


class RunIdFilter(logging.Filter):
    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


def setup_logging(run_id: str, level: Optional[int] = None) -> RunIdFilter:
    logging.basicConfig(
        level=level or logging.INFO,
        format="%(asctime)s [%(levelname)s] [run_id=%(run_id)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    run_filter = RunIdFilter(run_id)
    logging.getLogger().addFilter(run_filter)
    return run_filter


def set_run_id(run_filter: RunIdFilter, run_id: str) -> None:
    run_filter.run_id = run_id
