# Services

## DLSDispatcher

Single point of contact service that takes in job meta-information
(say, a data collection ID), a processing recipe, a list of recipes,
or pointers to recipes stored elsewhere, and mangles these into something
that can be processed by downstream services.

Subscribes to the `processing_recipe` queue.

## DLSFileWatcher

A service that waits for files to arrive on disk and notifies interested
parties when they do, or don't.

Subscribes to the `filewatcher` queue.

## DLSArchiver

## DLSImages

## DLSISPyB

## DLSCluster

A service to interface zocalo with functions to start new jobs on the clusters.
It has a number of different modes:
* Watch for files where the names follow a linear numeric pattern, eg. "template%05d.cbf"
  with indices 0 to 1800.
* Watch for a given list of files.
* Watch for hdf5 files written in SWMR mode. This will examine the hdf5 master file to
  determine the number of images to watch for, and then look to see whether each image has
  been written to file.

It will send notifications to relevant output streams when images are observed to be
present on disk, e.g.:
* `first` - notify when the first file is observed
* `last` - notify when the last file has been observed
* `select-N` - notify for _N_ evenly spaced files
* `every-N` - notify every _Nth_ file

Subscribes to the `cluster.submission` queue.

## DLSPerImageAnalysis

This service performs per-image analysis on individual images. For every message received
in the `per_image_analysis` queue a single image will be analysed. This calls the
[`work()`](https://github.com/dials/dials/blob/c28a1d0c868805faffeaf5f20b6fdc6efd2877d1/command_line/find_spots_server.py#L71-L260) function of the
[`dials.find_spots_server`](https://dials.github.io/documentation/programs/dials_find_spots_server.html).

There is a secondary function that subscribes to the `per_image_analysis.hdf5_select`
queue that can generate valid PIA messages for EIGER/HDF5 data collections. It needs to
know the location of the master file, the image range and how many images should be
picked.

Subscribes to the `per_image_analysis` and `per_image_analysis.hdf5_select` queues.

## DLSTrigger

## DLSValidation

## DLSXrayCentering

## DLSNexusParser

## DLSNotifyGda

A service that forwards per-image-analysis results to GDA via a UDP socket.

Subscribes to the `notify_gda` queue.

## DLSMimas

Business logic component. Given a data collection ID and some description
of event circumstances (beamline, experiment description, start or end of
scan) this service decides what recipes should be run with what settings.

Subscribes to the `mimas` queue.

## DLSMimasBacklog

A service to monitor the mimas.held backlog queue and drip-feed them into
the live processing queue as long as there isn't a cluster backlog.

Subscribes to the `mimas.held` queue and the `transient.statistics.cluster` topic.