from __future__ import annotations

import argparse
import json
import pathlib

import ispyb.sqlalchemy
import sqlalchemy
from ispyb.sqlalchemy import (
    BLSample,
    BLSession,
    DataCollection,
    DataCollectionGroup,
    Detector,
    DiffractionPlan,
    ExperimentType,
)
from sqlalchemy.sql.sqltypes import Float, Integer, String


def run():
    parser = argparse.ArgumentParser(
        description="Generate JSON-formatted data collection metadata for use with mmcif-gen tool for PDB deposition."
    )
    parser.add_argument(
        "dcid", type=int, help="DCID value of referenced data collection"
    )
    parser.add_argument(
        "--json",
        type=pathlib.Path,
        help="Output JSON file name for writing data collection metadata",
    )
    args = parser.parse_args()

    ispyb_sessionmaker = sqlalchemy.orm.sessionmaker(
        bind=sqlalchemy.create_engine(
            ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
        )
    )

    with ispyb_sessionmaker() as session:
        dc = (
            session.query(
                DataCollection.dataCollectionId.cast(Integer).label("DATACOLLECTIONID"),
                DataCollection.startTime.cast(String).label("STARTTIME"),
                BLSession.beamLineName.label("BEAMLINENAME"),
                DataCollection.averageTemperature.cast(Float).label(
                    "AVERAGETEMPERATURE"
                ),
                DataCollection.wavelength.cast(Float).label("WAVELENGTH"),
                Detector.detectorManufacturer.label("DETECTORMANUFACTURER"),
                Detector.detectorModel.label("DETECTORMODEL"),
                DataCollection.beamSizeAtSampleX.cast(Float).label("BEAMSIZEATSAMPLEX"),
                DataCollection.beamSizeAtSampleY.cast(Float).label("BEAMSIZEATSAMPLEY"),
                DiffractionPlan.monochromator.label("PDBX_MONOCHROMATOR"),
                ExperimentType.name.label("PDBX_DIFFRN_PROTOCOL"),
            )
            .join(
                DataCollectionGroup,
                DataCollectionGroup.dataCollectionGroupId
                == DataCollection.dataCollectionGroupId,
            )
            .join(BLSession, BLSession.sessionId == DataCollection.SESSIONID)
            .join(BLSample, BLSample.blSampleId == DataCollection.BLSAMPLEID)
            .outerjoin(
                ExperimentType,
                ExperimentType.experimentTypeId == DataCollectionGroup.experimentTypeId,
            )
            .outerjoin(
                DiffractionPlan,
                DiffractionPlan.diffractionPlanId == BLSample.diffractionPlanId,
            )
            .outerjoin(Detector, Detector.detectorId == DataCollection.detectorId)
            .filter(DataCollection.dataCollectionId == args.dcid)
            .one()
        )
        dc_info = dict(dc)
        dc_info.update(
            {
                "PDBX_SOURCE": "SYNCHROTRON",
                "PDBX_SYNCHROTRON": "DIAMOND",
                "PDBX_TYPE": "DIAMOND BEAMLINE " + dc_info["BEAMLINENAME"].upper(),
                "PDBX_DETECTOR": "PIXEL",
                "PDBX_SCATTERING_TYPE": "X-RAY",
                "PDBX_MONOCHROMATOR_OR_LAUE_M_L": None,
                "PDBX_AMBIENT_TEMP": dc_info["AVERAGETEMPERATURE"],
            }
        )
    results = {
        "datacollections": [
            dc_info,
        ]
    }
    json_txt = json.dumps(results, indent=4)

    print(json_txt)
    if args.json:
        args.json.write_text(json_txt)


if __name__ == "__main__":
    run()
