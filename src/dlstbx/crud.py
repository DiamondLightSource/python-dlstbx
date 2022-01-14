import os
from typing import List

import ispyb.sqlalchemy as models
import sqlalchemy.orm

from dlstbx import schemas


def get_auto_proc_program(
    auto_proc_program_id: int,
    session: sqlalchemy.orm.session.Session,
) -> models.AutoProcProgram:
    query = session.query(models.AutoProcProgram).filter(
        models.AutoProcProgram.autoProcProgramId == auto_proc_program_id
    )
    return query.one()


def update_auto_proc_program(
    auto_proc_program_id: int,
    auto_proc_program: schemas.AutoProcProgram,
    session: sqlalchemy.orm.session.Session,
) -> models.AutoProcProgram:
    app = auto_proc_program
    db_app = get_auto_proc_program(auto_proc_program_id, session)
    db_app.processingCommandLine = app.command_line
    db_app.processingPrograms = app.programs
    db_app.processingStatus = app.status
    db_app.processingMessage = app.message
    db_app.processingStartTime = app.start_time
    db_app.processingEndTime = app.end_time
    session.add(db_app)
    return db_app


def create_auto_proc_program(
    auto_proc_program: schemas.AutoProcProgram,
    session: sqlalchemy.orm.session.Session,
) -> models.AutoProcProgram:
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
    return db_app


def create_mxmrrun(
    mxmrrun: schemas.MXMRRun,
    db_app: models.AutoProcProgram,
    session: sqlalchemy.orm.session.Session,
) -> models.MXMRRun:
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
    return db_mxmrrun


def create_blobs(
    blobs: List[schemas.Blob],
    db_mxmrrun: models.MXMRRun,
    session: sqlalchemy.orm.session.Session,
) -> List[models.MXMRRunBlob]:
    db_blobs: List[models.MXMRRunBlob] = [
        models.MXMRRunBlob(
            MXMRRun=db_mxmrrun,
            view1=blob.view1,
            view2=blob.view2,
            view3=blob.view3,
            filePath=os.fspath(blob.filepath) if blob.filepath else None,
            x=blob.xyz[0],
            y=blob.xyz[1],
            z=blob.xyz[2],
            height=blob.height,
            occupancy=blob.occupancy,
            nearestAtomName=blob.nearest_atom.name if blob.nearest_atom else None,
            nearestAtomChainId=blob.nearest_atom.chain_id
            if blob.nearest_atom
            else None,
            nearestAtomResName=blob.nearest_atom.res_name
            if blob.nearest_atom
            else None,
            nearestAtomResSeq=blob.nearest_atom.res_seq if blob.nearest_atom else None,
            nearestAtomDistance=blob.nearest_atom_distance,
            mapType=blob.map_type,
        )
        for blob in blobs
    ]
    session.add_all(db_blobs)
    return db_blobs


def create_attachments(
    attachments: List[schemas.Attachment],
    db_app: models.AutoProcProgram,
    session: sqlalchemy.orm.session.Session,
) -> List[models.AutoProcProgramAttachment]:

    db_attachments: List[models.AutoProcProgramAttachment] = [
        models.AutoProcProgramAttachment(
            AutoProcProgram=db_app,
            fileType=att.file_type,
            fileName=att.file_name,
            filePath=os.fspath(att.file_path),
            recordTimeStamp=att.timestamp,
            importanceRank=att.importance_rank,
        )
        for att in attachments
    ]
    session.add_all(db_attachments)
    return db_attachments


def insert_dimple_result(
    mxmrrun: schemas.MXMRRun,
    blobs: List[schemas.Blob],
    auto_proc_program: schemas.AutoProcProgram,
    attachments: List[schemas.Attachment],
    session: sqlalchemy.orm.session.Session,
) -> models.MXMRRun:
    app_id = mxmrrun.auto_proc_program_id
    if app_id:
        db_app = update_auto_proc_program(app_id, auto_proc_program, session)
    else:
        db_app = create_auto_proc_program(auto_proc_program, session)
    db_mxmrrun = create_mxmrrun(mxmrrun, db_app, session)
    create_blobs(blobs, db_mxmrrun, session)
    create_attachments(attachments, db_app, session)
    session.commit()
    return db_mxmrrun
