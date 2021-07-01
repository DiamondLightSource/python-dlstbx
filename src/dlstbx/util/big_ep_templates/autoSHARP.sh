{% if not singularity_image %}
. /etc/profile.d/modules.sh

module load sharp
{% endif %}

run_autoSHARP.sh -seq {{ seqin_filename }} {% if resol_low and resol_high %} -R {{ resol_low }} {{ resol_high }}{% endif %} \
-scaled -spgr {{ spacegroup }} -ha {{ atom }} -nsit {{ nsites }} \
{% for data in datasets %} -wvl {{ data.wavelength }} {{ data.name }} {{ data.fp }} {{ data.fpp }} -mtz {{ data.mtz }} {% endfor %}
