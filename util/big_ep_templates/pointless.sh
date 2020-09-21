. /etc/profile.d/modules.sh

module load {{ ccp4_module }}

pointless hklin {{ input_hkl }} hklout {{ hklin }} << eof > pointless.log
spacegroup {{ spacegroup }}
{% if resol_high %}
resolution high {{ resol_high }}
{% endif %}
#reindex h,k,l
eof

