import os
from typing import List

import ispyb.sqlalchemy as models
import sqlalchemy.orm

from dlstbx import schemas


def insert_dimple_result(
    mxmrrun: schemas.MXMRRun,
    blobs: List[schemas.Blob],
    auto_proc_program: schemas.AutoProcProgram,
    attachments: List[schemas.Attachment],
    session: sqlalchemy.orm.session.Session,
):
    app = auto_proc_program
    db_app = models.AutoProcProgram(
        processingCommandLine=app.command_line,
        processingPrograms=app.programs,
        processingStatus=app.status,
        processingMessage=app.message,
        processingStartTime=app.start_time,
        processingEndTime=app.end_time,
    )
    session.add(db_app)

    db_mxmrrun = models.MXMRRun(
        autoProcScalingId=mxmrrun.auto_proc_scaling_id,
        rValueStart=mxmrrun.rwork_start,
        rValueEnd=mxmrrun.rwork_end,
        rFreeValueStart=mxmrrun.rfree_start,
        rFreeValueEnd=mxmrrun.rfree_end,
        LLG=mxmrrun.LLG,
        TFZ=mxmrrun.TFZ,
        spaceGroup=mxmrrun.space_group,
        AutoProcProgram=db_app,
    )
    session.add(db_mxmrrun)

    db_blobs: List[models.MXMRRunBlob] = [
        models.MXMRRunBlob(
            MXMRRun=db_mxmrrun,
            view1=blob.view1,
            view2=blob.view2,
            view3=blob.view3,
            filePath=os.fspath(blob.filepath),
            x=blob.xyz[0],
            y=blob.xyz[1],
            z=blob.xyz[2],
            height=blob.height,
            occupancy=blob.occupancy,
            nearestAtomName=blob.nearest_atom.name,
            nearestAtomChainId=blob.nearest_atom.chain_id,
            nearestAtomResName=blob.nearest_atom.res_name,
            nearestAtomResSeq=blob.nearest_atom.res_seq,
            nearestAtomDistance=blob.nearest_atom_distance,
            mapType=blob.map_type.value,
        )
        for blob in blobs
    ]
    session.add_all(db_blobs)

    db_attachments: List[models.AutoProcProgramAttachment] = [
        models.AutoProcProgramAttachment(
            AutoProcProgram=db_app,
            fileType=att.file_type.value,
            fileName=att.file_name,
            filePath=os.fspath(att.file_path),
            recordTimeStamp=att.timestamp,
            importanceRank=att.importance_rank,
        )
        for att in attachments
    ]
    session.add_all(db_attachments)

    session.commit()
    return db_mxmrrun
