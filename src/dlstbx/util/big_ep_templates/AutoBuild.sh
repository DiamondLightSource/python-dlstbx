#!/bin/bash
{% if singularity_image %}
unset LIBTBX_BUILD
unset PYTHONPATH
unset LD_LIDRARY_PATH
source ${PHENIX}/phenix_env.sh
{% else %}
. /etc/profile.d/modules.sh

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

phenix.autobuild after_autosol=True quick=True rebuild_in_place=False seq_file={{ wd }}/{{ seqin_filename }} n_cycle_build_max=1 n_cycle_rebuild_max=1 nproc=4 clean_up=True check_wait_time=30.0 max_wait_time=300.0 wait_between_submit_time=30.0 > phenix_autobuild.log

