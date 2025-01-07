#!/bin/bash
{% if singularity_image %}
unset LIBTBX_BUILD
unset PYTHONPATH
unset LD_LIDRARY_PATH
source ${CCP4}/bin/ccp4.setup-sh
source ${SHARP_home}/setup.sh
{% else %}
. /etc/profile.d/modules.sh

module load sharp
{% endif %}

run_autoSHARP.sh -seq {{ seqin_filename }} {% if resol_low and resol_high %} -R {{ resol_low }} {{ resol_high }}{% endif %} \
-scaled -spgr {{ spacegroup }} -ha {{ atom }} -nsit {{ nsites }} \
{% for data in datasets %} -wvl {{ data.wavelength }} {{ data.name }} {{ data.fp }} {{ data.fpp }} -mtz {{ data.mtz }} {% endfor %}
