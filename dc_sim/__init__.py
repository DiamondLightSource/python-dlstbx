#!/usr/bin/env python
#
#    A program to simulate a data collection
#
#    PLEASE NOTE:
#    This script must run as user gda2, and should probably run on a control machine
#
#    This program will:
#     * insert new entries into the datacollection table using the DbserverClient.py script
#     * copy images from the source data collection
#     * run the scripts RunAtStartOfDataCollection.sh, RunAfterEveryImage.sh, and RunAtEndOfDataCollection.sh
#       at appropriate times.
#

from __future__ import print_function
from __future__ import absolute_import
import sys, os, os.path, subprocess, shutil, logging
import tempfile
import dlstbx.dc_sim.mydb
import getopt
import re
import errno
import datetime

# "Globals"
logger = None

# Constants
DATABASE_HOST='ws096'
DATABASE_USER='ispyb4a_db'
TNSNAME='ws096'

MX_SCRIPTS_BINDIR='/dls_sw/apps/mx-scripts/bin'
DBSERVER_SRCDIR='/dls_sw/apps/mx-scripts/ispyb-dbserver/src'
DBSERVER_HOST='sci-serv3'
DBSERVER_PORT='1994'
DBSCHEMA='ispyb'

def f(_v):
    if _v is None:
        return float('nan')
    else:
        return float(_v)

def i(_v):
    if _v is None:
        return -1
    else:
        return int(_v)

def s(_v):
    if _v is None:
        return "null"
    else:
        return str(_v)

def copy_via_temp_file(source, destination):
  dest_dir, dest_file = os.path.split(destination)
  temp_dest_file = '.tmp.' + dest_file
  temp_destination = os.path.join(dest_dir, temp_dest_file)
  shutil.copyfile(source, temp_destination)
  os.rename(temp_destination, destination)

def populate_blsample_xml_template(_row):
    temp = blsample_xml % (
        s(_row['name']),
        s(_row['code']),
        s(_row['location']),
        f(_row['holderlength']),
        f(_row['looplength']),
        s(_row['looptype']),
        f(_row['wirewidth']),
        s(_row['comments']),
        s(_row['blsamplestatus']),
        i(_row['isinsamplechanger']),
        s(_row['lastknowncenteringposition']))

    # remove lines with null, nan and -1 values:
    temp = re.sub("\<.*\>null\</.*\>\n", "", temp)
    temp = re.sub("\<.*\>nan\</.*\>\n", "", temp)
    return re.sub("\<.*\>-1\</.*\>\n", "", temp)


def populate_dcg_xml_template(_row, _sessionid, _blsample_id):
    now = datetime.datetime.now()
    nowstr = now.strftime("%Y-%m-%d %H:%M:%S")
    blsample_id_elem = ""
    if _blsample_id != None:
        blsample_id_elem = "<blSampleId>%d</blSampleId>\n" % _blsample_id

    temp = dcg_temp_xml % (_sessionid, blsample_id_elem,
                           s(_row['experimenttype']), nowstr,
                           s(_row['crystalclass']), s(_row['detectormode']))

    # remove lines with null, nan and -1 values:
    temp = re.sub("\<.*\>null\</.*\>\n", "", temp)
    temp = re.sub("\<.*\>nan\</.*\>\n", "", temp)
    return re.sub("\<.*\>-1\</.*\>\n", "", temp)

def populate_grid_info_xml_template(_row, _dcgid):
    temp = grid_info_temp_xml % (
        _dcgid, f(_row['dx_mm']),
        f(_row['dy_mm']),
        i(_row['steps_x']),
        i(_row['steps_y']),
        f(_row['pixelspermicronx']),
        f(_row['pixelspermicrony']),
        f(_row['snapshot_offsetxpixel']),
        f(_row['snapshot_offsetypixel']),
        s(_row['orientation']))

    # remove lines with null, nan and -1 values:
    temp = re.sub("\<.*\>null\</.*\>\n", "", temp)
    temp = re.sub("\<.*\>nan\</.*\>\n", "", temp)
    return re.sub("\<.*\>-1\</.*\>\n", "", temp)

def populate_dc_xml_template(_row, _sessionid, _dcg_id, _no_images,
                             _dir, _prefix, _run_number, _xtal_snapshot_path,
                             _blsample_id):
    now = datetime.datetime.now()
    nowstr = now.strftime("%Y-%m-%d %H:%M:%S")
    suffix = _row['imagesuffix']
    file_template = "%s_%d_####.%s" %(_prefix, _run_number, suffix)
    blsample_id_elem = ""
    if _blsample_id != None:
        blsample_id_elem = "<blSampleId>%d</blSampleId>\n" % _blsample_id

    temp = dc_temp_xml % (_sessionid, _dcg_id, blsample_id_elem, _run_number, nowstr, s(_row['runstatus']), f(_row['axisstart']),
                          f(_row['axisend']), f(_row['axisrange']), f(_row['overlap']), _no_images, i(_row['startimagenumber']),
                          i(_row['numberofpasses']), f(_row['exposuretime']), _dir, _prefix, suffix, file_template,
                          f(_row['wavelength']), f(_row['resolution']), f(_row['detectordistance']), f(_row['xbeam']),
                          f(_row['ybeam']), i(_row['printableforreport']), f(_row['slitgapvertical']),
                          f(_row['slitgaphorizontal']), f(_row['transmission']), s(_row['synchrotronmode']),
                          _xtal_snapshot_path[0], _xtal_snapshot_path[1], _xtal_snapshot_path[2], _xtal_snapshot_path[3],
                          s(_row['rotationaxis']), f(_row['phistart']), f(_row['chistart']), f(_row['kappastart']),
                          f(_row['omegastart']), f(_row['undulatorgap1']), f(_row['beamsizeatsamplex']),
                          f(_row['beamsizeatsampley']), f(_row['flux']), i(_row['focalspotsizeatsamplex']), i(_row['focalspotsizeatsampley']))

    # remove lines with null, nan and -1 values:
    temp = re.sub("\<.*\>null\</.*\>\n", "", temp)
    temp = re.sub("\<.*\>nan\</.*\>\n", "", temp)
    return re.sub("\<.*\>-1\</.*\>\n", "", temp)

blsample_xml = '''<?xml version="1.0" ?>
<BLSample>
<name>%s</name>
<code>%s</code>
<location>%s</location>
<holderLength>%.2f</holderLength>
<loopLength>%.2f</loopLength>
<loopType>%s</loopType>
<wireWidth>%.2f</wireWidth>
<comments>%s</comments>
<blSampleStatus>%s</blSampleStatus>
<isInSampleChanger>%d</isInSampleChanger>
<lastKnownCenteringPosition>%s</lastKnownCenteringPosition>
</BLSample>'''

dcg_temp_xml='''<?xml version="1.0" ?>
<DataCollectionGroup>
<sessionId>%d</sessionId>
%s<comments>Simulated datacollection.</comments>
<experimentType>%s</experimentType>
<startTime>%s</startTime>
<crystalClass>%s</crystalClass>
<detectorMode>%s</detectorMode>
</DataCollectionGroup>'''

grid_info_temp_xml='''<?xml version="1.0" ?>
<GridInfo>
<dataCollectionGroupId>%d</dataCollectionGroupId>
<dx_mm>%.2f</dx_mm>
<dy_mm>%.2f</dy_mm>
<steps_x>%d</steps_x>
<steps_y>%d</steps_y>
<pixelsPerMicronX>%.4f</pixelsPerMicronX>
<pixelsPerMicronY>%.4f</pixelsPerMicronY>
<snapshot_offsetXPixel>%.4f</snapshot_offsetXPixel>
<snapshot_offsetYPixel>%.4f</snapshot_offsetYPixel>
<orientation>%s</orientation>
</GridInfo>
'''

dc_temp_xml='''<?xml version="1.0" ?>
<DataCollection>
<sessionId>%d</sessionId>
<dataCollectionGroupId>%d</dataCollectionGroupId>
%s<dataCollectionNumber>%d</dataCollectionNumber>
<startTime>%s</startTime>
<runStatus>%s</runStatus>
<axisStart>%.2f</axisStart>
<axisEnd>%.2f</axisEnd>
<axisRange>%.2f</axisRange>
<overlap>%.2f</overlap>
<numberOfImages>%d</numberOfImages>
<startImageNumber>%d</startImageNumber>
<numberOfPasses>%d</numberOfPasses>
<exposureTime>%.3f</exposureTime>
<imageDirectory>%s</imageDirectory>
<imagePrefix>%s</imagePrefix>
<imageSuffix>%s</imageSuffix>
<fileTemplate>%s</fileTemplate>
<wavelength>%.6f</wavelength>
<resolution>%.2f</resolution>
<detectorDistance>%.6f</detectorDistance>
<xBeam>%.6f</xBeam>
<yBeam>%.6f</yBeam>
<comments>Simulated datacollection.</comments>
<printableForReport>%d</printableForReport>
<slitGapVertical>%.6f</slitGapVertical>
<slitGapHorizontal>%.6f</slitGapHorizontal>
<transmission>%.6f</transmission>
<synchrotronMode>%s</synchrotronMode>
<xtalSnapshotFullPath1>%s</xtalSnapshotFullPath1>
<xtalSnapshotFullPath2>%s</xtalSnapshotFullPath2>
<xtalSnapshotFullPath3>%s</xtalSnapshotFullPath3>
<xtalSnapshotFullPath4>%s</xtalSnapshotFullPath4>
<rotationAxis>%s</rotationAxis>
<phiStart>%.1f</phiStart>
<chiStart>%.1f</chiStart>
<kappaStart>%.1f</kappaStart>
<omegaStart>%.1f</omegaStart>
<undulatorGap1>%.6f</undulatorGap1>
<beamSizeAtSampleX>%.2f</beamSizeAtSampleX>
<beamSizeAtSampleY>%.2f</beamSizeAtSampleY>
<flux>%.6f</flux>
<focalSpotSizeAtSampleX>%d</focalSpotSizeAtSampleX>
<focalSpotSizeAtSampleY>%d</focalSpotSizeAtSampleY>
</DataCollection>'''

dc_endtime_temp_xml='''<?xml version="1.0" ?>
<DataCollection>
<dataCollectionId>%d</dataCollectionId>
<endTime>%s</endTime>
</DataCollection>'''

dcg_endtime_temp_xml='''<?xml version="1.0" ?>
<DataCollectionGroup>
<dataCollectionGroupId>%d</dataCollectionGroupId>
<endTime>%s</endTime>
</DataCollectionGroup>'''

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def retrieve_sessionid(_db, _dbschema, _visit):
    rows = _db.doQuery("SELECT s.sessionid "\
                             "FROM %s.BLSession s "\
                             "  INNER JOIN %s.Proposal p ON p.proposalid = s.proposalid "\
                             "WHERE concat(p.proposalcode, p.proposalnumber, '-', s.visit_number)= '%s'"\
                              % (_dbschema,_dbschema,_visit))
    if rows[0][0] is None:
        sys.exit("Could not find sessionid for visit %s" % _visit)
    return int(rows[0][0])


def retrieve_datacollection_group_values(_db, _dbschema, _src_dcgid):
    _db.cursor.execute("SELECT comments, blsampleid, experimenttype, starttime, endtime, crystalclass, detectormode, actualsamplebarcode, "\
                        "actualsampleslotincontainer, actualcontainerbarcode, actualcontainerslotinsc, workflowid, xtalsnapshotfullpath "\
                        "FROM %s.DataCollectionGroup "\
                        "WHERE datacollectiongroupid=%d" % (_dbschema, _src_dcgid))

    desc = [d[0] for d in _db.cursor.description]
    result = [dict(zip(desc,line)) for line in _db.cursor]

    if len(result) == 0:
        sys.exit("Could not find datacollectiongroup %s" % _src_dcgid)
    return result[0]

def retrieve_grid_info_values(_db, _dbschema, _src_dcgid):
    _db.cursor.execute("SELECT dx_mm, dy_mm, steps_x, steps_y, pixelspermicronx, pixelspermicrony, "\
                       "snapshot_offsetxpixel, snapshot_offsetypixel, orientation "\
            "FROM %s.GridInfo "\
            "WHERE datacollectiongroupid=%d" % (_dbschema, _src_dcgid))

    desc = [d[0] for d in _db.cursor.description]
    result = [dict(zip(desc,line)) for line in _db.cursor]

    if len(result) == 0:
        return None
    return result[0]

def retrieve_datacollection_values(_db, _dbschema, _sessionid, _dir, _prefix, _run_number):
    prefix_line = 'AND imageprefix is NULL '
    if not _prefix is None:
        prefix_line = "AND imageprefix='%s' " % _prefix


    _db.cursor.execute("SELECT datacollectionid, datacollectiongroupid, blsampleid, startimagenumber, "\
                       "xtalsnapshotfullpath1, xtalsnapshotfullpath2, xtalsnapshotfullpath3, xtalsnapshotfullpath4, "\
                       "runstatus, axisstart, axisend, axisrange, overlap, numberofimages, startimagenumber, "\
                       "numberofpasses, exposuretime, imagesuffix, filetemplate, "\
                       "wavelength, resolution, detectordistance, xbeam, ybeam, comments, printableforreport, "\
                       "slitgapvertical, slitgaphorizontal, transmission, synchrotronmode, "\
                       "rotationaxis, phistart, chistart, kappastart, omegastart, undulatorgap1, "\
                       "beamsizeatsamplex, beamsizeatsampley, flux, focalspotsizeatsamplex, focalspotsizeatsampley "\
                       "FROM %s.DataCollection "\
                       "WHERE sessionid=%d "\
                       "AND imagedirectory='%s' "\
                       "%s "\
                       "AND datacollectionnumber=%d "% (_dbschema,_sessionid,_dir+"/", prefix_line, _run_number))

    desc = [d[0] for d in _db.cursor.description]
    result = [dict(zip(desc,line)) for line in _db.cursor]

    if result[0]['datacollectionid'] is None:
        sys.exit("Could not find the datacollectionid for visit %s" % _src_visit)
    if result[0]['startimagenumber'] is None:
        sys.exit("Could not find the startimagenumber for the row")
    return result[0]

def retrieve_blsample_values(_db, _dbschema, _src_blsampleid):
    _db.cursor.execute("SELECT blsampleid, name, code, location, holderlength, looplength, looptype, wirewidth, comments, "\
                       "blsamplestatus, isinsamplechanger, lastknowncenteringposition "\
                       "FROM %s.BLSample "\
                       "WHERE blsampleid=%d " % (_dbschema, _src_blsampleid))

    desc = [d[0] for d in _db.cursor.description]
    result = [dict(zip(desc,line)) for line in _db.cursor]

    if result[0]['blsampleid'] is None:
        sys.exit("Could not find the blsampleid for visit %s" % _src_visit)

    return result[0]

def retrieve_no_images(_db, _dbschema, _dcid):
    no_images = None
    rows = _db.doQuery("SELECT numberOfImages from %s.DataCollection where datacollectionid=%d" % (_dbschema, _dcid))
    if rows[0][0] is None:
        sys.exit("Could not find the number of images for datacollectionid %d" % _dcid)
    if int(rows[0][0]) is 0:
        sys.exit("Could not find the number of images for datacollectionid %d" % _dcid)
    return int(rows[0][0])

def retrieve_max_dcnumber(_db, _dbschema, _sessionid, _dest_dir, _dest_prefix):
    rows = _db.doQuery("SELECT max(datacollectionnumber) "\
                             "FROM %s.DataCollection "\
                             "WHERE sessionid=%d "\
                             "AND imagedirectory='%s' "\
                             "AND imageprefix='%s'" % (_dbschema,_sessionid,_dest_dir+"/", _dest_prefix))
    return rows[0][0]

def dest_dir(_beamline):
    '''Determines destination directory'''
    import uuid
    random_str = str(uuid.uuid4())
    year = datetime.datetime.now().year
    month = datetime.datetime.now().month
    day = datetime.datetime.now().day
    hour = datetime.datetime.now().hour
    minute = datetime.datetime.now().minute
    second = datetime.datetime.now().second
    for cm_dir in os.listdir('/dls/{0}/data/{1}'.format(_beamline, year)):
        if cm_dir.startswith('nt18231'):
            return '/dls/{0}/data/{1}/{2}/tmp/{3}-{4}-{5}/{6}{7}{8}-{9}'.format(_beamline, year, cm_dir, year, month, day, hour, minute, second, random_str)


def scenario(_test_name):
    '''provide the test scenario, returns False if test is not valid'''
    import dlstbx.dc_sim.definitions as df
    if _test_name in df.tests:
        source_directory = df.tests[_test_name]['src_dir']
        source_prefix = df.tests[_test_name]['src_prefix']
        source_run_numbers = df.tests[_test_name]['src_run_num']
        if 'use_sample_id' in df.tests[_test_name]:
            sample_id = df.tests[_test_name]['use_sample_id']
        else:
            sample_id = None
        return [source_directory, source_prefix, source_run_numbers, sample_id]
    else:
        return False


def simulate(_db, _dbschema,
             _dest_visit, _beamline, _data_src_dir, _src_dir, _src_visit, _src_prefix, _src_run_number,
             _dest_prefix, _dest_visit_dir, _dest_dir, _sample_id, _auto_proc='Yes'):
    logging.getLogger().debug("(SQL) Getting the source sessionid")
    src_sessionid = retrieve_sessionid(_db, _dbschema, _src_visit)

    logging.getLogger().debug("(SQL) Getting values from the source datacollection record")
    row = retrieve_datacollection_values(_db, _dbschema, src_sessionid, _src_dir, _src_prefix, _src_run_number)
    src_dcid = int(row['datacollectionid'])
    src_dcgid = int(row['datacollectiongroupid'])
    start_img_number = int(row['startimagenumber'])
    src_xtal_snapshot_path = [row['xtalsnapshotfullpath1'], row['xtalsnapshotfullpath2'], row['xtalsnapshotfullpath3'], row['xtalsnapshotfullpath4']]

    logging.getLogger().debug("(SQL) Getting the number of images")
    no_images = retrieve_no_images(_db, _dbschema, src_dcid)
    logging.getLogger().debug("(ANS) Got %d" % no_images)

    # Get the sessionid for the dest_visit
    logging.getLogger().debug("(SQL) Getting the destination sessionid")
    sessionid = retrieve_sessionid(_db, _dbschema, _dest_visit)

    # Get the highest run number for the datacollections of this dest_visit with the particular img.dir and prefix
    logging.getLogger().debug("(SQL) Getting the currently highest run number for this img. directory + prefix")
    run_number = retrieve_max_dcnumber(_db, _dbschema, sessionid, _dest_dir, _dest_prefix)
    if run_number is None:
        run_number = 1
    else:
        run_number = int(run_number) + 1

    logging.getLogger().debug("(SQL) Getting values from the source datacollectiongroup record")
    dcg_row = retrieve_datacollection_group_values(_db, _dbschema, src_dcgid)

    src_blsampleid = dcg_row['blsampleid']

    logging.getLogger().debug("(filesystem) Copy the xtal snapshot(s) (if any) from source to target directories")
    dest_xtal_snapshot_path = ["", "", "", ""]
    for x in xrange(0, 4):
        if src_xtal_snapshot_path[x] is not None:
            if os.path.exists(src_xtal_snapshot_path[x]):
                png = re.sub("^.*/(.*)$", _dest_dir + r"/\1", src_xtal_snapshot_path[x])
                dest_xtal_snapshot_path[x] = re.sub("^"+_dest_visit_dir, _dest_visit_dir+"/jpegs", png)
                dir = os.path.dirname(dest_xtal_snapshot_path[x])
                logging.getLogger().debug("(filesystem) ... 'mkdir -p' %s" % dir)
                mkdir_p(dir)
                logging.getLogger().debug("(filesystem) ... copying %s to %s" % (src_xtal_snapshot_path[x], dest_xtal_snapshot_path[x]))
                copy_via_temp_file(src_xtal_snapshot_path[x], dest_xtal_snapshot_path[x])

    # Get a blsampleId either from a copy of the blsample used by the src dc or use the blsampleId provided on the command-line
    blsample_id = None
    if src_blsampleid != None:
        if _sample_id is None:

            logging.getLogger().debug("(SQL) Getting values from the source blsample record")
            bls_row = retrieve_blsample_values(_db, _dbschema, int(src_blsampleid))

            # Produce a BLSample.xml file from the template
            logging.getLogger().debug("(filesystem) Creating a temporary blsample XML file in the /tmp folder")

            blsample_xml = populate_blsample_xml_template(bls_row)
            print(blsample_xml)

            f = tempfile.NamedTemporaryFile(suffix='.xml', prefix='blsample', dir='/tmp', delete=False)
            xml_fname = f.name
            f.write(blsample_xml)
            f.close()

            # Ingest the blsample.xml file data using the DbserverClient
            logging.getLogger().debug("(dbserver) Ingest the blsample XML")
            subprocess.check_call([os.path.join(DBSERVER_SRCDIR, 'DbserverClient.py'), '-h', DBSERVER_HOST, \
                             '-p', DBSERVER_PORT, '-i',  xml_fname, '-d', '-o', '/tmp/test.log'])

            # Extract the blsampleId from the output
            logging.getLogger().debug("(filesystem) Read the returned blsampleid from output file")
            f = file('/tmp/test.log', 'r')
            xml = f.read()
            m = re.search("<blSampleId>(\d+)</blSampleId>", xml)
            if m:
                blsample_id = int(m.groups()[0])
            else:
                sys.exit("No blsampleid found in output")

        else:
            blsample_id = _sample_id

    # Prouce a DataCollectionGroup.xml file from the template
    logging.getLogger().debug("(filesystem) Creating a temporary datacollectiongroup XML file in the /tmp folder")
    dcg_xml = populate_dcg_xml_template(dcg_row, sessionid, blsample_id)

    f = tempfile.NamedTemporaryFile(suffix='.xml', prefix='datacollectiongroup', dir='/tmp', delete=False)
    xml_fname = f.name
    f.write(dcg_xml)
    f.close()

    # Ingest the DataCollectionGroup.xml file data using the DbserverClient
    logging.getLogger().debug("(dbserver) Ingest the datacollectiongroup XML")
    subprocess.check_call([os.path.join(DBSERVER_SRCDIR, 'DbserverClient.py'), '-h', DBSERVER_HOST, \
                             '-p', DBSERVER_PORT, '-i',  xml_fname, '-d', '-o', '/tmp/test.log'])

    # Extract the datacollectiongroupId from the output
    logging.getLogger().debug("(filesystem) Read the returned datacollectiongroupid from output file")
    f=file('/tmp/test.log', 'r')
    xml = f.read()
    datacollectiongroupid = None
    m = re.search("<dataCollectionGroupId>(\d+)</dataCollectionGroupId>", xml)
    if m:
        datacollectiongroupid = int(m.groups()[0])
    else:
        sys.exit("No datacollectiongroupid found in output")


    # Get the grid info values associated with the source dcg
    gi_row = retrieve_grid_info_values(_db, _dbschema, src_dcgid)

    # Prouce a GridInfo.xml file from the template if the source DataCollectionGroup has one:
    if gi_row is not None:
        logging.getLogger().debug("(filesystem) Creating a temporary gridinfo XML file in the /tmp folder")
        dcg_xml = populate_grid_info_xml_template(gi_row, datacollectiongroupid)

        f = tempfile.NamedTemporaryFile(suffix='.xml', prefix='gridinfo', dir='/tmp', delete=False)
        xml_fname = f.name
        f.write(dcg_xml)
        f.close()

        # Ingest the GridInfo.xml file data using the DbserverClient
        logging.getLogger().debug("(dbserver) Ingest the gridinfo XML")
        subprocess.check_call([os.path.join(DBSERVER_SRCDIR, 'DbserverClient.py'), '-h', DBSERVER_HOST, \
                             '-p', DBSERVER_PORT, '-i',  xml_fname, '-d', '-o', '/tmp/test.log'])

        # Extract the gridinfoId from the output
        logging.getLogger().debug("(filesystem) Read the returned gridinfoid from output file")
        f=file('/tmp/test.log', 'r')
        xml = f.read()
        gridinfoid = None
        m = re.search("<gridInfoId>(\d+)</gridInfoId>", xml)
        if m:
            gridinfoid = int(m.groups()[0])
        else:
            sys.exit("No gridinfoid found in output")

    # Produce a DataCollection.xml file from the template and use the new run number
    logging.getLogger().debug("(filesystem) Creating a temporary datacollection XML file in the /tmp folder")
    dc_xml = populate_dc_xml_template(row, sessionid, datacollectiongroupid, no_images, _dest_dir+"/", _dest_prefix,
                                      run_number, dest_xtal_snapshot_path, blsample_id)
    # print dc_xml

    f = tempfile.NamedTemporaryFile(suffix='.xml', prefix='datacollection', dir='/tmp', delete=False)
    xml_fname = f.name
    f.write(dc_xml)
    f.close()

    # Ingest the DataCollection.xml file data using the DbserverClient
    logging.getLogger().debug("(dbserver) Ingest the datacollection XML")
    subprocess.check_call([os.path.join(DBSERVER_SRCDIR, 'DbserverClient.py'), '-h', DBSERVER_HOST, \
                             '-p', DBSERVER_PORT, '-i',  xml_fname, '-d', '-o', '/tmp/test.log'])

    # Extract the datacollectionId from the output
    logging.getLogger().debug("(filesystem) Read the returned datacollectionid from output file")
    f=file('/tmp/test.log', 'r')
    xml = f.read()
    datacollectionid = None
    m = re.search("<dataCollectionId>(\d+)</dataCollectionId>", xml)
    if m:
        datacollectionid = int(m.groups()[0])
    else:
        sys.exit("No datacollectionid found in output")

    run_at_params = ['automaticProcessing_' + _auto_proc,\
                     str(datacollectionid), _dest_visit_dir, _dest_prefix + '_' + str(run_number) + "_####.cbf",\
                     _dest_dir + '/', _dest_prefix + '_' + str(run_number) + '_', 'cbf']

    logging.getLogger().debug('(bash script) %s/RunAtStartOfCollect-%s.sh %s %s %s %s %s %s %s' % (MX_SCRIPTS_BINDIR, _beamline, run_at_params[0], run_at_params[1], run_at_params[2], run_at_params[3], run_at_params[4], run_at_params[5], run_at_params[6]))
    subprocess.check_call(['%s/RunAtStartOfCollect-%s.sh %s %s %s %s %s %s %s' % (MX_SCRIPTS_BINDIR, _beamline, run_at_params[0], run_at_params[1], run_at_params[2], run_at_params[3], run_at_params[4], run_at_params[5], run_at_params[6])], shell=True)

    # Also copy images one by one from source to destination directory.
    for x in xrange(start_img_number, start_img_number + no_images):
        img_number = "%04d" % x
        src_prefix = ""
        if not _src_prefix is None:
            src_prefix = _src_prefix
        src_fname = "%s_%d_%s.cbf" % (src_prefix, _src_run_number, str(img_number))
        dest_fname = "%s_%d_%s.cbf" % (_dest_prefix, run_number, str(img_number))
        src = os.path.join(_data_src_dir, src_fname)
        target = os.path.join(_dest_dir, dest_fname)
        logging.getLogger().info("(filesystem) Copy file %s to %s" % (src, target))
        copy_via_temp_file(src, target)

        # Run bash script
        # automaticProcessing_Yes 4310 /dls/i03/data/2008/0-0 98 /dls/i03/data/2008/0-0/0611/test1_MS_2_098.img img

        # FIXME in here optionally trigger ZOCALO processing

        image_params = ['automaticProcessing_' + _auto_proc, str(datacollectionid), _dest_visit_dir,\
                         str(x), os.path.join(_dest_dir, dest_fname), 'cbf']
        logging.getLogger().debug('(bash script) %s/RunAfterEveryImage-%s.sh %s %s %s %s %s %s' % (MX_SCRIPTS_BINDIR, _beamline, image_params[0], image_params[1], image_params[2], image_params[3], image_params[4], image_params[5]))
        subprocess.check_call(['%s/RunAfterEveryImage-%s.sh %s %s %s %s %s %s' % (MX_SCRIPTS_BINDIR, _beamline, image_params[0], image_params[1], image_params[2], image_params[3], image_params[4], image_params[5])] + image_params, shell=True)


    # Populate a datacollection XML file
    now = datetime.datetime.now()
    nowstr = now.strftime("%Y-%m-%d %H:%M:%S")

    dc_xml = dc_endtime_temp_xml % (datacollectionid, nowstr)
    print(dc_xml)
    f = tempfile.NamedTemporaryFile(suffix='.xml', prefix='datacollection', dir='/tmp', delete=False)
    xml_fname = f.name
    f.write(dc_xml)
    f.close()

    # Ingest the DataCollection.xml file data using the DbserverClient
    logging.getLogger().debug("(dbserver) Ingest the datacollection XML to update with the d.c. end time")
    subprocess.check_call([os.path.join(DBSERVER_SRCDIR, 'DbserverClient.py'), '-h', DBSERVER_HOST, \
                             '-p', DBSERVER_PORT, '-i',  xml_fname, '-d', '-o', '/tmp/test.log'])


    # Populate a datacollectiongroup XML file
    now = datetime.datetime.now()
    nowstr = now.strftime("%Y-%m-%d %H:%M:%S")

    dcg_xml = dcg_endtime_temp_xml % (datacollectiongroupid, nowstr)
    print(dcg_xml)
    f = tempfile.NamedTemporaryFile(suffix='.xml', prefix='datacollectiongroup', dir='/tmp', delete=False)
    xml_fname = f.name
    f.write(dcg_xml)
    f.close()

    # Ingest the DataCollectionGroup.xml file data using the DbserverClient
    logging.getLogger().debug("(dbserver) Ingest the datacollectiongroup XML to update with the d.c.g. end time")
    subprocess.check_call([os.path.join(DBSERVER_SRCDIR, 'DbserverClient.py'), '-h', DBSERVER_HOST, \
                             '-p', DBSERVER_PORT, '-i',  xml_fname, '-d', '-o', '/tmp/test.log'])

    logging.getLogger().debug('(bash script) %s/RunAtEndOfCollect-%s.sh %s %s %s %s %s %s' % (MX_SCRIPTS_BINDIR, _beamline, run_at_params[0], run_at_params[1], run_at_params[2], run_at_params[3], run_at_params[4], run_at_params[5]))
    subprocess.check_call(['%s/RunAtEndOfCollect-%s.sh %s %s %s %s %s %s' % (MX_SCRIPTS_BINDIR, _beamline, run_at_params[0], run_at_params[1], run_at_params[2], run_at_params[3], run_at_params[4], run_at_params[5]) ], shell=True)

    # Log datacollectionid to beamline specific location and ouput useful data into dictionary
    with open("/dls/tmp/" + _beamline + "/dc_sim.log","a+") as f:
        f.write(str(datacollectionid) + " : " + nowstr +"\n")
        print("Data collection logged in: " + "/dls/tmp/" + _beamline + "/dc_sim.log")
        #sim_output_dict = {"beamline": _beamline, "date": nowstr, "dcid": str(datacollectionid)}
        return datacollectionid

def printHelp(msg=None):
    if msg != None:
        print("Error: %s" % msg)
    print("Options:")
    print("  --host=<hostname>")
    print("  --port=<port number>")
    print("  --data_src_dir=<dirctory containing images>")
    print("  --src_dir=<datacollection source directory>")
    print("  --src_prefix=<source datacollection prefix>")
    print("  --src_run_number=<source datacollection run number>")
    print("  --dest_dir=<datacollection destination directory>")
    print("  --dest_prefix=<destination prefix>")
    print("  --automatic_processing=<Yes|No>")
    print("  -d or --debug")
    print("  --help")
    print("")
    print("Example: ")
    print("python dc_sim.py --dbserver_host=sci-serv3 --dbserver_port=1994 --dbhost=duoserv12 "\
          "--dbuser=ispyb4a_db --dbschema=ispyb4a_db --tnsname=ispyb "\
          "--src_dir=/dls/i03/data/2013/cm5926-1/0130/thau3 "\
          "--dest_dir=/dls/p45/data/2013/cm5952-2 --src_run_number=1 --src_prefix=test")

def call_sim(test_name, beamline):

    # Default parameters
    dbhost = DATABASE_HOST
    dbuser = DATABASE_USER
    tnsname = TNSNAME
    dbschema = DBSCHEMA

    data_src_dir = None
    src_run_number = None
    dest_visit_dir = None
    dest_prefix = None
    dest_visit = None
    dcid_list = []
    debug = False


    # Fetch scenario data from definitions by accessing scenario function
    if scenario(test_name)!= False:     
        src_dir = scenario(test_name)[0]
        sample_id = scenario(test_name)[3]
        src_prefix = scenario(test_name)[1]
    else:
        sys.exit("Not a valid test scenario")

    # Calculate the destination directory - get beamline as command line parameter
    dest_dir = dest_dir(beamline)

    # Checks for mandatory parameters
    if src_dir is None:
        printHelp("src_dir is a mandatory parameter")
        sys.exit(0)
    elif dest_dir is None:
        printHelp("dest_dir is a mandatory parameter")
        sys.exit(0)
    if data_src_dir is None:
        data_src_dir = src_dir

    # Extract necessary info from the source directory path
    src_beamline = None
    m1 = re.search("(/dls/(\S+?)/data/\d+/)(\S+)", src_dir)
    if m1:
        src_beamline = m1.groups()[1]
        subdir = m1.groups()[2]
        m2 = re.search("^(\S+?)/", subdir)
        if m2:
            src_visit = m2.groups()[0]
            src_visit_dir = m1.groups()[0] + src_visit
        elif (subdir is not None) and (subdir != ""):
            src_visit = subdir
            src_visit_dir = m1.groups()[0] + src_visit

    if (src_beamline is None) or (src_visit_dir is None) or (src_visit is None):
        sys.exit("ERROR: The src_dir parameter does not appear to contain a valid visit directory.")

    # Extract necessary info from the destination directory path
    dest_beamline = None
    m1 = re.search("(/dls/(\S+?)/data/\d+/)(\S+)", dest_dir)
    if m1:
        dest_beamline = m1.groups()[1]
        subdir = m1.groups()[2]
        m2 = re.search("^(\S+?)/", subdir)
        if m2:
            dest_visit = m2.groups()[0]
            dest_visit_dir = m1.groups()[0] + dest_visit
        elif (subdir is not None) and (subdir != ""):
            dest_visit = subdir
            dest_visit_dir = m1.groups()[0] + dest_visit

    if (dest_beamline is None) or (dest_visit_dir is None) or (dest_visit is None):
        sys.exit("ERROR: The dest_dir parameter does not appear to contain a valid visit directory.")

    # Configure logging
    logger = logging.getLogger()
    if debug == True:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    formatter = logging.Formatter('* %(asctime)s [id=%(thread)d] <%(levelname)s> %(message)s')
    hdlr = logging.StreamHandler(sys.stdout)
    hdlr.setFormatter(formatter)
    logging.getLogger().addHandler(hdlr)


    start_script = "%s/RunAtStartOfCollect-%s.sh" % (MX_SCRIPTS_BINDIR, dest_beamline)
    if not os.path.exists(start_script):
        logging.getLogger().error("The file %s was not found." % start_script)
        sys.exit()
    per_img_script = "%s/RunAfterEveryImage-%s.sh" % (MX_SCRIPTS_BINDIR, dest_beamline)
    if not os.path.exists(per_img_script):
        logging.getLogger().error("The file %s was not found." % per_img_script)
        sys.exit()
    end_script = "%s/RunAtEndOfCollect-%s.sh" % (MX_SCRIPTS_BINDIR, dest_beamline)
    if not os.path.exists(end_script):
        logging.getLogger().error("The file %s was not found." % end_script)
        sys.exit()

    # Create destination directory
    logging.getLogger().debug("Creating directory %s" % dest_dir)
    mkdir_p(dest_dir)
    if os.path.isdir(dest_dir):
        logging.getLogger().info("Directory %s created successfully" % dest_dir)
    else:
        logging.getLogger().error("Creating directory %s failed" % dest_dir)

    db = dlstbx.dc_sim.mydb.DB()
    

    for src_run_number in scenario(test_name)[2]:
        for src_prefix in scenario(test_name)[1]:
            if dest_prefix is None:
                dest_prefix = src_prefix
                   
            dcid = simulate(db, dbschema, dest_visit, dest_beamline, data_src_dir, src_dir, src_visit, src_prefix, src_run_number, dest_prefix, dest_visit_dir, dest_dir, sample_id)
            return dcid_list.append(dcid)


