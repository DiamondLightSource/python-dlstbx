from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from typing import Optional

import iotbx.pdb
from scitbx.array_family import flex

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProteinInfo:
    proteinId: int
    proposalId: int
    hazardGroup: int
    containmentLevel: int
    bltimeStamp: datetime.datetime
    name: str
    acronym: str
    description: Optional[str] = None
    safetyLevel: Optional[str] = None
    molecularMass: Optional[int] = None
    proteinType: Optional[str] = None
    personId: Optional[int] = None
    sequence: Optional[str] = None
    isCreatedBySampleSheet: Optional[bool] = False
    MOD_ID: Optional[str] = None
    componentTypeId: Optional[int] = None
    concentrationTypeId: Optional[int] = None
    global_: Optional[int] = 0
    density: Optional[float] = None
    abundance: Optional[float] = None
    isotropy: Optional[str] = None


@dataclass(frozen=True)
class PDBFileOrCode:
    filepath: Optional[str] = None
    code: Optional[str] = None
    source: Optional[str] = None

    def __str__(self):
        return str(self.filepath) if self.filepath else str(self.code)


def trim_pdb_bfactors(
    pdb_in: str,
    pdb_out: str,
    atom_selection: Optional[str] = None,
    set_b_iso: Optional[float] = None,
):
    h_all = iotbx.pdb.input(pdb_in).construct_hierarchy()
    if atom_selection:
        sel_cache = h_all.atom_selection_cache()
        selection = sel_cache.iselection(atom_selection)
        logging.info(
            f"Selecting {selection.size()} out of {h_all.atoms().size()} atoms where {atom_selection}"
        )
        h_sel = h_all.select(selection)
    else:
        h_sel = h_all
    if set_b_iso is not None:
        xrs = h_sel.extract_xray_structure()
        xrs.set_b_iso(values=flex.double(len(xrs.scatterers()), set_b_iso))
        h_sel.adopt_xray_structure(xrs)
    h_sel.write_pdb_file(pdb_out)
