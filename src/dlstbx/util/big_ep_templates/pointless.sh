#!/bin/bash
{% if singularity_image %}
unset LIBTBX_BUILD
unset PYTHONPATH
unset LD_LIDRARY_PATH
source ${CCP4}/bin/ccp4.setup-sh
{% else %}
. /etc/profile.d/modules.sh

module load ccp4
{% endif %}

pointless hklin {{ input_hkl }} hklout {{ hklin }} << eof > pointless.log
spacegroup {{ spacegroup }}
{% if resol_high %}
resolution high {{ resol_high }}
{% endif %}
#reindex h,k,l
eof

