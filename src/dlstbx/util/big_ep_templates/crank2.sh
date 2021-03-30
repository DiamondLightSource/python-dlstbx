. /etc/profile.d/modules.sh

module load {{ ccp4_module }}

cat > crank2_config.xml << EOF
<?xml version='1.0' encoding='utf-8'?>
<process>crank<param>
{% if datasets|length > 1 %}
    <target>MAD</target>
{% else %}
    <target>SAD</target>
{% endif %}
  </param>
  <inp>
    <model typ="substr">
      <attr_set_by_crank>['xname']</attr_set_by_crank>
      <solvent_content>{{ solv }}</solvent_content>
      <exp_num_atoms>{{ nsites }}</exp_num_atoms>
{% for data in datasets %}
      <atomtype>{{ atom }}
         <fp>{{ data.fp }}</fp>
         <fpp>{{ data.fpp }}</fpp>
         <d_name>{{ data.name }}</d_name>
      </atomtype>
{% endfor %}
    </model>
    <sequence typ="protein">
      <attr_set_by_crank>['xname']</attr_set_by_crank>
      <file typ="fasta">{{ seqin }}</file>
      <monomers_asym>1</monomers_asym>
      <residues_mon>{{ _nres }}</residues_mon>
    </sequence>
{% for data in datasets %}
    <fsigf typ="plus">
      <attr_set_by_crank>['xname',]</attr_set_by_crank>
      <dname>{{ data.name }}</dname>
      <file typ="mtz">{{ hklin }}</file>
      <i>{{ data.column_list[0][0] }}</i>
      <sigi>{{ data.column_list[1][0] }}</sigi>
    </fsigf>
    <fsigf typ="minus">
      <attr_set_by_crank>['xname',]</attr_set_by_crank>
      <dname>{{ data.name }}</dname>
      <file typ="mtz">{{ hklin }}</file>
      <i>{{ data.column_list[2][0] }}</i>
      <sigi>{{ data.column_list[3][0] }}</sigi>
    </fsigf>
{% endfor %}
  </inp>
  <process>faest</process>
  <process>substrdet</process>
  <process>phas</process>
  <process>handdet</process>
  <process>dmfull</process>
{% if datasets|length > 1 %}
  <process>mbref<process>mb<program>buccaneer<key>
          <jobs>8</jobs>
        </key>
      </program>
    </process>
    <param><bigcyc>10</bigcyc></param>
  </process>
  {% if enableArpWarp %}
  <process>mbref<program>arpwarp
      </program>
  </process>
  {% endif %}
{% else %}
  <process>comb_phdmmb<process>mb<program>buccaneer<key>
          <jobs>8</jobs>
        </key>
      </program>
    </process>
  </process>
{% endif %}
</process>
EOF

{% set parse_logfile = [_wd, 'crank2.log']|join('/') %}

stdbuf -oL {{ crank2_bin }} --xmlin=crank2_config.xml > {{ parse_logfile }}
