. /etc/profile.d/modules.sh

module load {{ sharp_module }}

cat > .autoSHARP << EOF

# ----------------------------------------
# General information about project
# ----------------------------------------
  autoSHARP_proj="autoSHARP"
  autoSHARP_jobi="0"
  autoSHARP_titl="autoSHARP processing run by big_ep"
  autoSHARP_type="MAD"
  autoSHARP_rate="5"
{% if seqin %}
  autoSHARP_pirf="{{ seqin_filename }}"
{% elif nres %}
  autoSHARP_nres="{{ nres }}"
{% endif %}
{% if spacegroup %}
  autoSHARP_spgr="{{ spacegroup }}"
{% endif %}
{% if resol_low %}
  autoSHARP_resl="{{ resol_low }}"
{% else %}
  autoSHARP_resl="9999.0"
{% endif %}
{% if resol_high %}
  autoSHARP_resh="{{ resol_high }}"
{% else %}
  autoSHARP_resh="0.01"
{% endif %}
  autoSHARP_nset="{{ datasets|length }}"
  autoSHARP_user="{{ user }}"
  autoSHARP_ulvl="3"
  autoSHARP_chtm="no"
  autoSHARP_csum="no"
# ----------------------------------------
# autoSHARP protocol information
# ----------------------------------------
  autoSHARP_EntryPoint="2"
  autoSHARP_EntryPoint3_Path="3"
  autoSHARP_EntryPoint3_Path3_Opt="8"
  autoSHARP_DetectPgm="shelx"
  autoSHARP_RunningType="ccp4i"

{% for data in datasets %}
# ----------------------------------------
# Dataset {{ data.index }}
# ----------------------------------------
  autoSHARP_iden_{{ data.index }}="{{ data.name }}"
  autoSHARP_wave_{{ data.index }}="{{ data.wavelength }}"
  autoSHARP_hatm_{{ data.index }}="{{ atom }}"
  {% if nsites %}
  autoSHARP_nsit_{{ data.index }}="{{ nsites }}"
  {% endif %}
  autoSHARP_sitf_{{ data.index }}=""
  autoSHARP_fone_{{ data.index }}="{{ data.fp }}"
  autoSHARP_ftwo_{{ data.index }}="{{ data.fpp }}"
  autoSHARP_fmid_{{ data.index }}="{{ data.F }}"
  autoSHARP_smid_{{ data.index }}="{{ data.SIGF }}"
  autoSHARP_dano_{{ data.index }}="{{ data.DANO }}"
  autoSHARP_sano_{{ data.index }}="{{ data.SIGDANO }}"
  autoSHARP_isym_{{ data.index }}="{{ data.ISYM }}"
  autoSHARP_dtyp_{{ data.index }}="MTZ"
  autoSHARP_data_{{ data.index }}="{{ data.mtz }}"
{% endfor %}
EOF

$SHARP_home/bin/sharp/detect.sh > LISTautoSHARP.html 2>&1
