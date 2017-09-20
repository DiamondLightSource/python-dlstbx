#!/usr/bin/env python2.7
"""
"""

import os
import sys
import time

import click
import logbook
import lz4
import numpy
from beamline import variables as blconfig
from detectorSubscriber import DetectorSubscriber

logger = logbook.Logger(os.path.basename(__file__))
logbook.StreamHandler(sys.stdout).push_application()
logbook.set_datetime_format("local")

class AdscStreamer(DetectorSubscriber):
    def __init__(self, top_directory, detector_address):
        super(AdscStreamer, self).__init__(detector_address)
        self.top_directory = top_directory
        self.series = 1 #probably should be coming form the detector
        import mflow
        self.stream = mflow.connect("tcp://127.0.0.1:9999", conn_type=mflow.BIND, mode=mflow.PUSH, receive_timeout=1, queue_size=1000)

    def start(self):
        #do not worry about this too much as spotter only looks at htype dimage-1.0
        part1 = {"htype":"dheader-1.0", "series":self.series, "header_detail":"basic"}
        part2 = {}
        self.publish(part1, part2)
    def run_file(self, data):
        import fabio
        if self.top_directory != None:
            split_dirs = data.split(os.sep)
            split_dirs[1] = self.top_directory
            data = os.sep.join(split_dirs)
        self.filename = data
        timeout = 60
        times = 0
        while times < timeout:
            if not os.path.exists(data):
                time.sleep(1)
                times += 1
                if times == timeout:
                    print "file not there yet..."
                elif times % 10 == 0:
                    print "waiting..."
            else:
                break
        file= fabio.open(data)
        header = file.header
        #part 1
        part1 = {"htype":"dimage-1.0"}
        part1["series"] = self.series
        part1["frame"] = int((data.split(".")[-2]).split("_")[-1])

        part2 = {"htype":"dimage_d-1.0", "type":"uint16", "size":"18874368", "shape":[int(header['SIZE1']), int(header['SIZE2'])], "encoding":"adsc"}
        part3 = lz4.dumps(file.data)
        part4 = {"htype":"dconfig-1.0", "start_time":0, "stop:time":int(file.header['ACC_TIME'])*1e9, "real_time": int(file.header['ACC_TIME'])*1e9}
        import hashlib
        m=hashlib.md5()
        m.update(part3)
        hash = m.hexdigest()
        part1["hash"] = hash
        self.publish(part1, part2, part3, part4, self.get_appendix(header))
    def get_template(self,filename):
        main_path = os.path.dirname(filename)
        splitext = os.path.splitext(os.path.basename(filename))
        file_template = splitext[0].split('_')
        file_template[-1] = "%03d"
        template = os.sep.join([main_path,'_'.join(file_template)+splitext[1]])
        return template
    def get_appendix(self, header):
        appendix = {}
        appendix['pixel_size'] = float(header['PIXEL_SIZE'])
        appendix['distance'] = float(header['DISTANCE']) / 1000. # send meters
        appendix['wavelength'] = float(header['WAVELENGTH'])
        appendix['beam_position_x'] = float(header['BEAM_CENTER_X']) / appendix['pixel_size'] # send pixels
        appendix['beam_position_y'] = float(header['BEAM_CENTER_Y']) / appendix['pixel_size'] # send pixels
        appendix['low_resolution_limit'] = 52.0
        appendix['high_resolution_limit'] = 1.0
        appendix['minimum_signal_height'] = None
        appendix['minimum_spot_area'] = 10
        appendix['osc_start'] = float(header['OSC_START'])
        appendix['deltaphi'] = float(header['OSC_RANGE'])
        appendix['two_theta'] = float(header['TWOTHETA'])
        appendix['tracking_id'] = header['DETECTOR_SN']
        appendix['file_template'] = self.get_template(self.filename)
        appendix['beamline'] = 'MX2' # TODO make this not-so-hard-coded!
        appendix['spotfinder'] = True
        return appendix
    def end(self):
        #do not worry about this too much as spotter only looks at htype dimage-1.0
        part1={"htype":"dseries_end-1.0","series":self.series}
        self.publish(part1)
        self.series += 1
    def publish(self, *parts):
        import json
        for part in parts:
            more_parts = (part!=reversed(parts).next())
            if isinstance(part, dict):
                self.stream.send(json.dumps(part),send_more=more_parts)
            else: # raw
                self.stream.send(part,send_more=more_parts)

@click.option('--url', help="address to connect to a detector, such as tcp://10.108.2.27:8877")
@click.option('--top_directory', help="replace top level directory ('data' of /data) with this - sans/current, for example")
@click.command()
def call_adsc_streamer(top_directory, url="tcp://127.0.0.1:5678"):
    adscStreamer = AdscStreamer(top_directory, detector_address = url)
    try:
        logger.info("Started on %s" % (blconfig.ID, ))
        adscStreamer.process()
    except KeyboardInterrupt:
        logger.info("Exiting gracefully on Ctrl+C")
    finally:
        adscStreamer.close()

if __name__ == '__main__':
    call_adsc_streamer()
