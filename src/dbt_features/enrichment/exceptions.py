"""Exceptions raised by the enrichment subsystem."""

from __future__ import annotations


class EnrichmentError(Exception):
    """Raised when warehouse enrichment cannot proceed.

    Distinct from per-feature-group failures (those are captured on the
    snapshot itself and rendered inline). This signals a setup-level
    problem: missing driver, unreadable profiles.yml, unsupported warehouse.
    """
