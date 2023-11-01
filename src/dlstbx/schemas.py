from __future__ import annotations

import datetime
import enum
import pathlib

import pydantic


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
    occupancy: pydantic.FiniteFloat | None
    nearest_atom: Atom | None
    nearest_atom_distance: pydantic.FiniteFloat | None
    filepath: pathlib.Path | None
    view1: str | None
    view2: str | None
    view3: str | None

    class Config:
        use_enum_values = True


class AutoProcProgram(pydantic.BaseModel):
    command_line: str = pydantic.Field(..., max_length=255)
    programs: str
    status: int
    message: str
    start_time: datetime.datetime
    end_time: datetime.datetime

    @pydantic.validator("command_line", pre=True)
    def command_line_length(cls, v, field):
        max_length = field.field_info.max_length
        if v and len(v) > max_length:
            v = v[:max_length]
        return v


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
    importance_rank: int | None

    class Config:
        use_enum_values = True


class MXMRRun(pydantic.BaseModel):
    auto_proc_scaling_id: int
    auto_proc_program_id: int | None
    rwork_start: pydantic.FiniteFloat
    rwork_end: pydantic.FiniteFloat
    rfree_start: pydantic.FiniteFloat
    rfree_end: pydantic.FiniteFloat
    space_group: str | None
    LLG: pydantic.FiniteFloat | None
    TFZ: pydantic.FiniteFloat | None


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
    centre_of_mass: tuple[float, ...] | None
    max_voxel: tuple[int, ...] | None
    max_count: float | None
    n_voxels: int | None
    total_count: float | None
    bounding_box: tuple[Coordinate2D, Coordinate2D] | tuple[
        Coordinate3D, Coordinate3D
    ] | None


class XrayCentring(pydantic.BaseModel):
    dcgid: pydantic.NonNegativeInt
    status: XrayCentringStatus
    type: XrayCentringType
    results: list[XrayCentringResult]
