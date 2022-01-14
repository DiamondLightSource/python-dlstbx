import datetime
import enum
import pathlib
from typing import Optional, Tuple

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
    xyz: Tuple[float, float, float]
    height: float
    map_type: MapType
    occupancy: Optional[float]
    nearest_atom: Optional[Atom]
    nearest_atom_distance: Optional[float]
    filepath: Optional[pathlib.Path]
    view1: Optional[str]
    view2: Optional[str]
    view3: Optional[str]

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
    importance_rank: Optional[int]

    class Config:
        use_enum_values = True


class MXMRRun(pydantic.BaseModel):
    auto_proc_scaling_id: int
    auto_proc_program_id: Optional[int]
    rwork_start: float
    rwork_end: float
    rfree_start: float
    rfree_end: float
    space_group: Optional[str]
    LLG: Optional[float]
    TFZ: Optional[float]
