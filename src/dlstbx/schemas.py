from __future__ import annotations

import datetime
import enum
import pathlib

import pydantic
from pydantic import ConfigDict


class MapType(enum.Enum):
    ANOMALOUS = "anomalous"
    DIFFERENCE = "difference"


class Atom(pydantic.BaseModel):
    name: str
    chain_id: str
    res_seq: int
    res_name: str


class Blob(pydantic.BaseModel):
    xyz: tuple[pydantic.FiniteFloat, pydantic.FiniteFloat, pydantic.FiniteFloat]
    height: pydantic.FiniteFloat
    map_type: MapType
    occupancy: pydantic.FiniteFloat | None = None
    nearest_atom: Atom | None = None
    nearest_atom_distance: pydantic.FiniteFloat | None = None
    filepath: pathlib.Path | None = None
    view1: str | None = None
    view2: str | None = None
    view3: str | None = None
    model_config = ConfigDict(use_enum_values=True)


class AutoProcProgram(pydantic.BaseModel):
    command_line: str
    programs: str
    status: int
    message: str
    start_time: datetime.datetime
    end_time: datetime.datetime

    @pydantic.field_validator("command_line", mode="before")
    def command_line_length(cls, v: str):
        return v[:255]


class AttachmentFileType(enum.Enum):
    LOG = "log"
    RESULT = "result"
    GRAPH = "graph"
    DEBUG = "debug"
    INPUT = "input"


class Attachment(pydantic.BaseModel):
    file_type: AttachmentFileType
    file_path: pathlib.Path
    file_name: str
    timestamp: datetime.datetime
    importance_rank: int | None = None
    model_config = ConfigDict(use_enum_values=True)


class MXMRRun(pydantic.BaseModel):
    auto_proc_scaling_id: int
    auto_proc_program_id: int | None = None
    rwork_start: pydantic.FiniteFloat
    rwork_end: pydantic.FiniteFloat
    rfree_start: pydantic.FiniteFloat
    rfree_end: pydantic.FiniteFloat
    space_group: str | None = None
    LLG: pydantic.FiniteFloat | None = None
    TFZ: pydantic.FiniteFloat | None = None


class XrayCentringStatus(enum.Enum):
    SUCCESS = "success"
    FAILED = "failed"
    PENDING = "pending"


class XrayCentringType(enum.Enum):
    _2D = "2d"
    _3D = "3d"


Coordinate2D = tuple[int, int]
Coordinate3D = tuple[int, int, int]


class XrayCentringResult(pydantic.BaseModel):
    centre_of_mass: tuple[float, ...] | None = None
    max_voxel: tuple[int, ...] | None = None
    max_count: float | None = None
    n_voxels: int | None = None
    total_count: float | None = None
    bounding_box: (
        tuple[Coordinate2D, Coordinate2D] | tuple[Coordinate3D, Coordinate3D] | None
    ) = None


class XrayCentring(pydantic.BaseModel):
    dcgid: pydantic.NonNegativeInt
    status: XrayCentringStatus
    type: XrayCentringType
    results: list[XrayCentringResult]
