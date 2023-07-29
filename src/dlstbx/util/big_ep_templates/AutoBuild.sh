{% if not singularity_image %}
. /etc/profile.d/modules.sh

module load global/cluster
module load phenix
{% endif %}

cat > autosol.eff << EOF
autosol {
  atom_type = {{ atom }}
  sites= {{ nsites }}
  quick = True
  seq_file = {% if seqin_filename %} {{ wd }}/{{ seqin_filename }} {% else %} Auto {% endif %}
  data = {{ wd }}/{{ autosol_hklin }}

{% for data in datasets %}
  wavelength {
    lambda = {{ data.wavelength }}
    f_prime = {{ data.fp }}
    f_double_prime = {{ data.fpp }}
    labels='{{ data.columns }}'
  }
{% endfor %}

{% if _spacegroup %}
  crystal_info {
    space_group = {{ spacegroup }}
    {% if resol_high %} resolution = {{ resol_high }} {% endif %}
    change_sg = True
    {% if nres %} residues = {{nres}} {% endif %} 
  }
{% endif %}

  model_building {
    build = False
  }

  general {
    thoroughness = quick
    nbatch = 8
    nproc = 8
    background = True
    clean_up = True
  }
}
EOF

phenix.autosol autosol.eff > phenix_autosol.log

phenix.autobuild after_autosol=True quick=True rebuild_in_place=False seq_file={{ wd }}/{{ seqin_filename }} n_cycle_build_max=1 n_cycle_rebuild_max=1 {%if not singularity_image %} run_command="qsub -N PNX_build -q medium.q -P {{ cluster_project }} -V -cwd -o {{ wd }}/.launch -e {{ wd }}/.launch"  queue_commands=". /etc/profile.d/modules.sh" queue_commands="module load phenix" last_process_is_local=False background=False {% endif %} nproc=4 clean_up=True check_wait_time=30.0 max_wait_time=300.0 wait_between_submit_time=30.0 > phenix_autobuild.log

