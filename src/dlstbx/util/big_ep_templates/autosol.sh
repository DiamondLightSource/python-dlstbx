. /etc/profile.d/modules.sh

module load {{ phenix_module }}
module load {{ ccp4_module }}

cat > autosol.eff << EOF
autosol {
  atom_type = {{ atom }}
  sites= {{ nsites }}
  quick = True
  seq_file = {% if seqin %} {{ seqin }} {% else %} Auto {% endif %}
  data = {{ autosol_hklin }}

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
    thoroughness = thorough
    nbatch = 8
    nproc = 8
    background = True
    clean_up = True
  }
}
EOF

phenix.autosol autosol.eff > phenix_autosol.log
