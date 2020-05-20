import getpass
import os
import re
import sys
import uuid
import xml.dom.minidom
from tqdm import tqdm

import dlstbx.util.cluster

# disable output buffering
sys.stdout = os.fdopen(sys.stdout.fileno(), "w", 0)

if getpass.getuser() != "gda2":
    sys.exit("dlstbx.fix_cluster_jobs can only be run as user 'gda2'")

clusters = {
    "scientific cluster": dlstbx.util.cluster.Cluster("dlscluster"),
    "testcluster": dlstbx.util.cluster.Cluster("dlstestcluster"),
}

actions = {"forkxds_job": "delete", "zocalo-svc": "delete"}


def get_resubmission_id_for_job(cluster, jobid):
    jobinfo = cluster.qstat_xml(["-j", jobid])
    assert not jobinfo.returncode and not jobinfo["timeout"], (
        "Could not read job information for job %s" % jobid
    )
    jobinfo = xml.dom.minidom.parseString(jobinfo.stdout)
    context = jobinfo.getElementsByTagName("JB_context")
    if context:
        context = {
            cvar.getElementsByTagName("VA_variable")[0]
            .firstChild.nodeValue: cvar.getElementsByTagName("VA_value")[0]
            .firstChild.nodeValue
            for cvar in context[0].getElementsByTagName("element")
        }
    else:
        context = {}
    return context.get("resubmission_id")


# Resubmitting an errored job is done as a multi-stage operation:
#  1. Every errored job is assigned a unique resubmission ID
#  2. The errored job plus resubmission ID is resubmitted in a held status
#  3. The original job is deleted
#  4. The resubmitted job is downgraded to low.q and released
# This protocol ensures that jobs can not be duplicated or lost by
# pressing Ctrl+C at the wrong time.

stats = dlstbx.util.cluster.ClusterStatistics()
for clustername, cluster in clusters.items():
    print("\nGathering statistics for", clustername)
    jobs, queues = stats.run_on(cluster, arguments=["-f", "-u", "gda2"])
    print("* found %d jobs on cluster" % len(jobs))
    if not jobs:
        continue

    held_jobs = [j for j in jobs if j["statecode"] == "hqw" and j["owner"] == "gda2"]
    print("* found %d jobs in hold state" % len(held_jobs))
    resubmission_db = {}
    if held_jobs:
        with tqdm(desc="retrieving held job information", total=len(held_jobs)) as bar:
            for j in held_jobs:
                resubmission_id = get_resubmission_id_for_job(cluster, str(j["ID"]))
                if resubmission_id:
                    resubmission_db[resubmission_id] = j["ID"]
                bar.update(1)

    error_db = {}
    errored_jobs = [j for j in jobs if j["statecode"] == "Eqw" and j["owner"] == "gda2"]

    print("\n* found %d jobs in error state" % len(errored_jobs))
    if not errored_jobs:
        continue
    names = {}
    for j in errored_jobs:
        names.setdefault(j["name"], []).append(j)
    for n in sorted(names):
        if actions.get(n) == "delete":
            print(" %3dx %s  will be deleted" % (len(names[n]), n))
            with tqdm(desc="deleting", total=len(names[n])) as bar:
                for j in names[n]:
                    cluster.qdel([j["ID"]])
                    bar.update(1)
        elif actions.get(n) == "resubmit" or n.startswith("zoc-"):
            print(" %3dx %s  will be resubmitted" % (len(names[n]), n))
            for j in names[n]:
                error_db[j["ID"]] = None
        else:
            print(" %3dx %s  will be left untouched" % (len(names[n]), n))

    print()
    print("* %d jobs identified for resubmission" % len(error_db))
    with tqdm(desc="loading/creating resubmission IDs", total=len(error_db)) as bar:
        for j in error_db:
            resubmission_id = get_resubmission_id_for_job(cluster, str(j))
            if resubmission_id:
                error_db[j] = resubmission_id
            else:
                error_db[j] = str(uuid.uuid4())
                cluster.qalter(j, ["-ac", "resubmission_id=" + error_db[j]])
            bar.update(1)

    removable_jobs = [j for j in error_db if error_db[j] in resubmission_db]
    if removable_jobs:
        with tqdm(desc="deleting surplus jobs", total=len(removable_jobs)) as bar:
            for j in removable_jobs:
                cluster.qdel([j])
                del error_db[j]
                bar.update(1)

    if resubmission_db:
        with tqdm(desc="retriggering jobs", total=len(resubmission_db)) as bar:
            for j in resubmission_db.values():
                trigger = cluster.qalter(j, ["-q", "low.q", "-h", "U"])
                assert not trigger.returncode and not trigger["timeout"], (
                    "Could not retrigger job %s" % j
                )
                bar.update(1)
    resubmission_db = {}

    if error_db:
        with tqdm(desc="requeueing failed jobs", total=len(error_db)) as bar:
            for j in error_db:
                resub = cluster.qresub(j, ["-h", "u"])
                assert (
                    not resub.returncode and not resub["timeout"]
                ), "Could not requeue job %s: %s" % (j, resub.stderr or resub.stdout)
                resubmission_id = re.search(
                    "Your job(?:-array)? ([0-9]+)[ .]", resub.stdout
                )
                if not resubmission_id:
                    raise RuntimeError(
                        "Could not requeue job %s: %s"
                        % (j, resub.stderr or resub.stdout)
                    )
                resubmission_id = resubmission_id.group(1)
                resubmission_db[error_db[j]] = resubmission_id
                cluster.qdel([j])
                bar.update(1)

    print()
    if resubmission_db:
        with tqdm(desc="retriggering failed jobs", total=len(resubmission_db)) as bar:
            for j in resubmission_db.values():
                trigger = cluster.qalter(j, ["-q", "low.q", "-h", "U"])
                assert not trigger.returncode and not trigger["timeout"], (
                    "Could not retrigger job %s" % j
                )
                bar.update(1)
