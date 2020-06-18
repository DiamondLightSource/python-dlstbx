# Services

## DLSDispatcher

Single point of contact service that takes in job meta-information
(say, a data collection ID), a processing recipe, a list of recipes,
or pointers to recipes stored elsewhere, and mangles these into something
that can be processed by downstream services.

## DLSFileWatcher

A service that waits for files to arrive on disk and notifies interested
parties when they do, or don't.

## DLSArchiver

## DLSImages

## DLSISPyB

## DLSCluster

## DLSPerImageAnalysis

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