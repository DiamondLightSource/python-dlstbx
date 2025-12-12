FROM dials/dials:latest

RUN yum install bzip2 libXxf86vm rrdtool git -y
RUN pip install patchelf

WORKDIR /dials/modules

COPY . dlstbx

RUN source /dials/dials \
  && pip install -e ./dlstbx --no-deps

RUN source /dials/dials \
  && sed -i'' 's|libtbx.conda|/dials/conda_base/condabin/conda|' "/dials/modules/dlstbx/src/dlstbx/requirements.py" \
  && libtbx.python /dials/modules/dlstbx/contrib/candygram.py  dials dials_data dials_research dlstbx dxtbx fast_dp screen19 sphinx xia2 zocalo \
  && conda install -y --file=/dials/modules/dlstbx/requirements.conda.txt python-relion \
  && pip3 install git+https://github.com/DiamondLightSource/python-workflows@diag_emptyheader

CMD ["dials.version"]
