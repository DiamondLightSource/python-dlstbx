FROM harbor.diamond.ac.uk/dockerhub-cache/dials/dials:latest

RUN yum install bzip2 libXxf86vm rrdtool git -y
RUN pip install patchelf

WORKDIR /dials/modules

COPY . dlstbx

RUN source /dials/dials \
  && pip install -e ./dlstbx --no-deps

RUN source /dials/dials \
  && sed -i'' 's|libtbx.conda|/dials/conda_base/condabin/conda|' "/dials/modules/dlstbx/src/dlstbx/requirements.py" \
  && libtbx.python /dials/modules/dlstbx/candygram/candygram.py zocalo dials dials_data dxtbx xia2 sphinx fast_dp screen19 dials_research dlstbx \
  && python3 /dials/modules/dlstbx/src/dlstbx/requirements.py python-relion -y \
  && pip3 install git+https://github.com/DiamondLightSource/python-workflows@diag_emptyheader

CMD ["dials.version"]
