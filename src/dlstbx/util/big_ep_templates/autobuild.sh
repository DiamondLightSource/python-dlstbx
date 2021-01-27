. /etc/profile.d/modules.sh

module load global/cluster
module load {{ phenix_module }}

phenix.autobuild after_autosol=True rebuild_in_place=False seq_file={{ seqin }} n_cycle_build_max=3 n_cycle_rebuild_max=5 run_command="qsub -N PNX_build -q low.q -P {{ cluster_project }} -V -cwd -o {{ workingdir }} -e {{ workingdir }}"  queue_commands=". /etc/profile.d/modules.sh" queue_commands="module load {{ phenix_module }}" nproc=4 last_process_is_local=False clean_up=True background=False check_wait_time=30.0 max_wait_time=300.0 wait_between_submit_time=30.0 > phenix_autobuild.log

