# Services

## DLSArchiver

A service that generates dropfiles for data collections, in order to allow archiving of
collected datafiles connected to a data collection.

Subscribes to the `archive.pattern` and `archive.filelist` queues.

## DLSCluster

A service to interface zocalo with functions to start new jobs on the clusters.

Subscribes to the `cluster.submission` queue.

## DLSClusterMonitor

A service to interface zocalo with functions to gather cluster statistics. 

Sends results to the `statistics.cluster` queue and the `transient.statistics.cluster`
topic.

## DLSController

A service to supervise other services, start new instances and shut down existing ones
depending on policy and demand. Checks the overall data processing infrastructure state
and ensures that everything is working fine and resources are deployed appropriately.
        
Subscribes to the `transient.status` and `transient.queue_status` topics.

## DLSDispatcher

Single point of contact service that takes in job meta-information
(say, a data collection ID), a processing recipe, a list of recipes,
or pointers to recipes stored elsewhere, and mangles these into something
that can be processed by downstream services.

Subscribes to the `processing_recipe` queue.

## DLSFileWatcher

A service that waits for files to arrive on disk and notifies interested
parties when they do, or don't.

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

Subscribes to the `filewatcher` queue.

## DLSImages

A service that generates images and thumbnails via
[`dials.export_bitmaps`](https://dials.github.io/documentation/programs/dials_export_bitmaps.html).
These may be used e.g. by SynchWeb for providing diffraction image previews for data
collections.

Subscribes to the `images` queue.

## DLSISPyB

A service that receives information to be written to ISPyB. The functionality is split up
into a number of `ispyb_command`s that each handle a specific use case. Supported
`ispyb_command`s include:
* `create_ispyb_job`: create a new entry in the `ProcessingJob` table
* `update_processing_status`: update the processing status for a given processing program
* `store_dimple_failure`
* `register_processing`
* `add_program_attachment`
* `add_program_message`
* `add_datacollection_attachment`
* `store_per_image_analysis_results`
* `insert_screening`
* `insert_screening_output`
* `insert_screening_output_lattice`
* `insert_screening_strategy`
* `insert_screening_strategy_wedge`
* `insert_screening_strategy_sub_wedge`
* `register_integration`
* `upsert_integration`
* `write_autoproc`
* `insert_scaling`
* `insert_mxmr_run`
* `insert_mxmr_run_blob`
* `retrieve_programs_for_job_id`
* `retrieve_program_attachments_for_program_id`
* `retrieve_proposal_title`
* `multipart_message`: The multipart_message command allows the recipe or client to
  specify a multi-stage operation. With this you can process a list of API calls,
  for example
    * do_upsert_processing
    * do_insert_scaling
    * do_upsert_integration
  Each API call may have a return value that can be stored. Multipart_message takes care
  of chaining and checkpointing to make the overall call near-ACID compliant.

Subscribes to the `ispyb_connector` queue.


## DLSMailer

A service that generates emails from messages. This is used for notifying beamline staff
of potential issues, e.g. image arriving late (or not at all) to disk.

Subscribes to the `mailnotification` queue.

## DLSMimas

Business logic component. Given a data collection ID and some description
of event circumstances (beamline, experiment description, start or end of
scan) this service decides what recipes should be run with what settings.

Subscribes to the `mimas` queue.

## DLSMimasBacklog

A service to monitor the mimas.held backlog queue and drip-feed them into
the live processing queue as long as there isn't a cluster backlog.

Subscribes to the `mimas.held` queue and the `transient.statistics.cluster` topic.

## DLSNexusParser

A service that answers questions about Nexus files. This currently only has one function,
which takes a single file and recursively finds all referenced files. This is used by the
[`archive-nexus`](https://gitlab.diamond.ac.uk/scisoft/zocalo/-/blob/master/recipes/archive-nexus.json)
recipe to identify all linked external files for a given master file.

Subscribes to the `nexusparser.find_related_files` queue.

## DLSNotifyGda

A service that forwards per-image-analysis results to GDA via a UDP socket.

Subscribes to the `notify_gda` queue.

## DLSPerImageAnalysis

This service performs per-image analysis on individual images. For every message received
in the `per_image_analysis` queue a single image will be analysed. This calls the
[`work()`](https://github.com/dials/dials/blob/c28a1d0c868805faffeaf5f20b6fdc6efd2877d1/command_line/find_spots_server.py#L71-L260) function of the
[`dials.find_spots_server`](https://dials.github.io/documentation/programs/dials_find_spots_server.html).

Subscribes to the `per_image_analysis` queue.

## DLSStatistics

A service to gather report statistics on and around zocalo. Writes results to
[RRDtool](https://oss.oetiker.ch/rrdtool/) files in `/dls_sw/apps/zocalo/statistics/`.

Subscribes to the `statistics.cluster` queue.

## DLSTrigger

A service that creates and runs downstream processing jobs after the successful completion
of some upstream data processing task. This service is made up of a number of trigger
functions that each handle a specific downstream task. Existing trigger functions include
`best`, `big_ep`,`big_ep_launcher`, `dimple`, `ep_predict`, `fast_ep`, `mr_predict`,
`mrbump`, `multiplex` and `screen_19_mx`.

Subscribes to the `trigger` queue.

## DLSValidation

A service that validates data collections against ISPyB and for internal consistency.

Subscribes to the `validation` queue.

## DLSXrayCentering

A service to aggregate per-image-analysis results and identify an X-ray centering solution
for a data collection.

Subscribes to the `reduce.xray_centering` queue.
