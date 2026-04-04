#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path


LOG_DIR = Path(__file__).resolve().parent / "logs"
MAIN_LOG_PATH = LOG_DIR / "lagerverwaltung.log"
PRINT_LOG_PATH = LOG_DIR / "print.log"
LEGACY_PRINT_LOG_PATH = LOG_DIR / "druck.log"
_CONFIGURED_LOGGERS: set[str] = set()


def _log_level() -> int:
    raw = os.environ.get("LAGERVERWALTUNG_LOG_LEVEL", "INFO").strip().upper()
    return getattr(logging, raw, logging.INFO)


def _log_path_for(name: str) -> Path:
    if name in {"label_print", "print"}:
        return PRINT_LOG_PATH
    return MAIN_LOG_PATH


def configure_logging(name: str = "main") -> Path:
    LOG_DIR.mkdir(exist_ok=True)

    logger_name = f"lagerverwaltung.{name}"
    if logger_name in _CONFIGURED_LOGGERS:
        return _log_path_for(name)

    logger = logging.getLogger(logger_name)
    logger.setLevel(_log_level())
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    primary_handler = RotatingFileHandler(_log_path_for(name), maxBytes=1_000_000, backupCount=5, encoding="utf-8")
    primary_handler.setFormatter(formatter)
    logger.addHandler(primary_handler)

    if name in {"label_print", "print"}:
        legacy_handler = RotatingFileHandler(LEGACY_PRINT_LOG_PATH, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
        legacy_handler.setFormatter(formatter)
        logger.addHandler(legacy_handler)

    logger.propagate = False

    _CONFIGURED_LOGGERS.add(logger_name)
    return _log_path_for(name)


def get_logger(name: str) -> logging.Logger:
    configure_logging(name)
    return logging.getLogger(f"lagerverwaltung.{name}")
