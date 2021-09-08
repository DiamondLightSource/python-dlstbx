import argparse
import time

from dlstbx.cluster_monitor import parse_db


def _parse_labels_to_string(labels):
    as_str = ""
    for k, v in labels.items():
        as_str += f'{k}="{v}",'
    return as_str[:-1]


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", action="store_true", dest="start")
    parser.add_argument("-e", "--end", action="store_true", dest="end")
    parser.add_argument("-c", "--command", dest="command")
    parser.add_argument("--id", dest="job_id")
    parser.add_argument("--pid", dest="program_id")
    parser.add_argument("-t", "--timestamp", dest="timestamp")
    parser.add_argument("--host", dest="host")
    parser.add_argument("--cluster", dest="cluster")
    parser.add_argument("--gpus", dest="num_gpus", type=int)
    parser.add_argument("--ranks", dest="num_ranks", type=int)
    args = parser.parse_args()

    labels = {
        "cluster": args.cluster,
        "host_name": args.host,
        "auto_proc_program_id": args.program_id,
        "cluster_job_id": args.job_id,
        "command": args.command,
    }

    labels_string = _parse_labels_to_string(labels)

    db_parser = parse_db.DBParser()

    if args.start:
        db_parser.insert(
            metric="clusters_current_job_count",
            metric_labels=labels_string,
            metric_type="gauge",
            metric_value=1,
            cluster_id=args.job_id,
            auto_proc_program_id=args.program_id,
            timestamp=time.time(),
        )
        if args.num_gpus:
            db_parser.insert(
                metric="current_gpus_in_use_count",
                metric_labels=labels_string,
                metric_type="gauge",
                metric_value=args.num_gpus,
                cluster_id=args.job_id,
                auto_proc_program_id=args.program_id,
                timestamp=time.time(),
            )
            db_parser.insert(
                metric="current_mpi_ranks_in_use_count",
                metric_labels=labels_string,
                metric_type="gauge",
                metric_value=args.num_ranks,
                cluster_id=args.job_id,
                auto_proc_program_id=args.program_id,
                timestamp=time.time(),
            )
        return
    if args.end:
        _now = time.time()
        db_parser.insert(
            metric="clusters_current_job_count",
            metric_labels=labels_string,
            metric_type="gauge",
            metric_value=-1,
            cluster_id=args.job_id,
            auto_proc_program_id=args.program_id,
            timestamp=_now,
            cluster_end_timestamp=_now,
        )
        if args.num_gpus:
            db_parser.insert(
                metric="current_gpus_in_use_count",
                metric_labels=labels_string,
                metric_type="gauge",
                metric_value=-args.num_gpus,
                cluster_id=args.job_id,
                auto_proc_program_id=args.program_id,
                timestamp=time.time(),
            )
            db_parser.insert(
                metric="current_mpi_ranks_in_use_count",
                metric_labels=labels_string,
                metric_type="gauge",
                metric_value=-args.num_ranks,
                cluster_id=args.job_id,
                auto_proc_program_id=args.program_id,
                timestamp=time.time(),
            )
        return
