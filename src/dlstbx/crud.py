from __future__ import annotations

import os
from typing import List

import ispyb.sqlalchemy as models
import sqlalchemy.orm

from dlstbx import schemas


def get_data_collection(
    dcid: int,
    session: sqlalchemy.orm.session.Session,
) -> models.DataCollection | None:
    query = session.query(models.DataCollection).filter(
        models.DataCollection.dataCollectionId == dcid
    )
    return query.first()


def get_gridinfo_for_dcid(
    dcid: int,
    dcgid: int,
    session: sqlalchemy.orm.session.Session,
) -> models.GridInfo | None:
    query = session.query(models.GridInfo).filter(
        models.GridInfo.dataCollectionId == dcid
    )
    result = query.first()
    if result:
        return result

    # legacy: search on dcgid
    query = session.query(models.GridInfo).filter(
        models.GridInfo.dataCollectionGroupId == dcgid
    )
    return query.first()


def get_blsession_for_dcid(
    dcid: int,
    session: sqlalchemy.orm.session.Session,
) -> models.BLSession | None:
    query = (
        session.query(models.BLSession)
        .join(models.DataCollectionGroup)
        .join(models.DataCollection)
        .filter(models.DataCollection.dataCollectionId == dcid)
    )
    return query.first()


def get_dcids_for_sample_id(
    sample_id: int, session: sqlalchemy.orm.session.Session
) -> list[int]:
    query = (
        session.query(models.DataCollection.dataCollectionId)
        .join(models.DataCollectionGroup)
        .filter(models.DataCollectionGroup.blSampleId == sample_id)
        .order_by(models.DataCollection.dataCollectionId)
    )
    return [r.dataCollectionId for r in query.all()]


def get_dcids_for_data_collection_group(
    dcgid: int, session: sqlalchemy.orm.session.Session
) -> list[int]:
    query = session.query(models.DataCollection.dataCollectionId).filter(
        models.DataCollection.dataCollectionGroupId == dcgid,
    )
    return [r.dataCollectionId for r in query.all()]


def get_dcids_for_same_directory(
    dcid: int, session: sqlalchemy.orm.session.Session
) -> list[int]:
    dc1 = sqlalchemy.orm.aliased(models.DataCollection)
    dc2 = sqlalchemy.orm.aliased(models.DataCollection)
    query = (
        session.query(dc2.dataCollectionId)
        .join(
            dc1,
            (dc1.imageDirectory == dc2.imageDirectory)
            & (dc1.dataCollectionId != dc2.dataCollectionId)
            & (dc1.imageDirectory is not None),
        )
        .filter(dc1.dataCollectionId == dcid)
    )
    return [r.dataCollectionId for r in query.all()]


def get_diffraction_plan_for_dcid(
    dcid: int, session: sqlalchemy.orm.session.Session
) -> models.DiffractionPlan | None:
    query = (
        session.query(models.DiffractionPlan)
        .join(models.BLSample)
        .join(models.DataCollectionGroup)
        .join(models.DataCollection)
        .filter(models.DataCollection.dataCollectionId == dcid)
    )
    return query.first()


def get_crystal_for_dcid(
    dcid: int, session: sqlalchemy.orm.session.Session
) -> models.Crystal | None:
    query = (
        session.query(models.Crystal)
        .join(models.BLSample)
        .join(models.DataCollectionGroup)
        .join(models.DataCollection)
        .filter(models.DataCollection.dataCollectionId == dcid)
    )
    return query.first()


def get_protein_for_dcid(
    dcid: int, session: sqlalchemy.orm.session.Session
) -> models.Protein | None:
    query = (
        session.query(models.Protein)
        .join(models.Crystal)
        .join(models.BLSample)
        .join(models.DataCollectionGroup)
        .join(models.DataCollection)
        .filter(models.DataCollection.dataCollectionId == dcid)
    )
    return query.first()


def get_priority_processing_for_sample_id(
    sample_id: int, session: sqlalchemy.orm.session.Session
) -> str | None:
    query = (
        session.query(models.ProcessingPipeline.name)
        .join(models.Container)
        .join(models.BLSample)
        .filter(models.BLSample.blSampleId == sample_id)
    )
    pipeline = query.first()
    if pipeline:
        return pipeline.name
    return None


def get_detector(
    detector_id: int,
    session: sqlalchemy.orm.session.Session,
) -> models.Detector | None:
    query = session.query(models.Detector).filter(
        models.Detector.detectorId == detector_id
    )
    return query.first()


def get_blsample(
    sample_id: int,
    session: sqlalchemy.orm.session.Session,
) -> models.BLSample | None:
    query = session.query(models.BLSample).filter(
        models.BLSample.blSampleId == sample_id
    )
    return query.first()


def get_run_status_for_dcid(
    dcid: int,
    session: sqlalchemy.orm.session.Session,
) -> str | None:
    query = session.query(models.DataCollection.runStatus).filter(
        models.DataCollection.dataCollectionId == dcid
    )
    return query.scalar()


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
            nearestAtomChainId=(
                blob.nearest_atom.chain_id if blob.nearest_atom else None
            ),
            nearestAtomResName=(
                blob.nearest_atom.res_name if blob.nearest_atom else None
            ),
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


def get_pdb_for_dcid(
    dcid: int,
    session: sqlalchemy.orm.session.Session,
) -> list[models.PDB]:
    query = (
        session.query(models.PDB)
        .join(
            models.ProteinHasPDB,
            models.ProteinHasPDB.pdbid == models.PDB.pdbId,
        )
        .join(
            models.Protein, models.Protein.proteinId == models.ProteinHasPDB.proteinid
        )
        .join(models.Crystal, models.Crystal.proteinId == models.Protein.proteinId)
        .join(models.BLSample, models.BLSample.crystalId == models.Crystal.crystalId)
        .join(
            models.DataCollection,
            models.DataCollection.BLSAMPLEID == models.BLSample.blSampleId,
        )
        .filter(models.DataCollection.dataCollectionId == dcid)
    )
    return query.all()


def get_ssx_events_for_dcid(
    dcid: int,
    session: sqlalchemy.orm.session.Session,
) -> list[models.Event]:
    query = (
        session.query(models.Event)
        .join(models.EventChain)
        .options(sqlalchemy.orm.joinedload(models.Event.EventType))
        .filter(models.EventChain.dataCollectionId == dcid)
    )
    return query.all()


def insert_xray_centring(
    xrc: schemas.XrayCentring,
    session: sqlalchemy.orm.session.Session,
) -> int:
    db_xrc = models.XrayCentring(
        dataCollectionGroupId=xrc.dcgid,
        status=xrc.status.value,
        xrayCentringType=xrc.type.value,
    )
    session.add(db_xrc)
    is_3d = xrc.type == schemas.XrayCentringType._3D
    for result in xrc.results:
        bb = result.bounding_box
        db_result = models.XrayCentringResult(
            XrayCentring=db_xrc,
            centreOfMassX=result.centre_of_mass[0] if result.centre_of_mass else None,
            centreOfMassY=result.centre_of_mass[1] if result.centre_of_mass else None,
            centreOfMassZ=(
                result.centre_of_mass[2] if result.centre_of_mass and is_3d else None
            ),
            maxVoxelX=result.max_voxel[0] if result.max_voxel else None,
            maxVoxelY=result.max_voxel[1] if result.max_voxel else None,
            maxVoxelZ=result.max_voxel[2] if result.max_voxel and is_3d else None,
            numberOfVoxels=result.n_voxels,
            totalCount=result.total_count,
            boundingBoxMinX=bb[0][0] if bb else None,
            boundingBoxMinY=bb[0][1] if bb else None,
            boundingBoxMinZ=bb[0][2] if bb and is_3d else None,  # type: ignore
            boundingBoxMaxX=bb[1][0] if bb else None,
            boundingBoxMaxY=bb[1][1] if bb else None,
            boundingBoxMaxZ=bb[1][2] if bb and is_3d else None,  # type: ignore
        )
        session.add(db_result)
    session.commit()
    return db_xrc.xrayCentringId
