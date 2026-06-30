"""Standalone (non-array) runner for the XChem collate pipeline.

``run`` performs, for a single labxchem visit, the same collation as the
``xchem_collate`` zocalo wrapper -- PanDDA2 postrun, model selection + soakDB
re-integration, optional Pipedream collate, XChemAlign collate and Fragalysis
upload -- but without the zocalo / ispyb machinery. Driven by the sibling
``run_xchem_collate.sh`` bash launcher.
"""

from __future__ import annotations
