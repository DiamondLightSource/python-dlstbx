FROM harbor.diamond.ac.uk/dockerhub-cache/dials/dials:latest

RUN yum install bzip2 libXxf86vm rrdtool -y

WORKDIR /dials/modules

COPY . dlstbx

RUN source /dials/dials \
  && pip install -e ./dlstbx --no-deps
RUN source /dials/dials \
  && sed -i'' 's|libtbx.conda|mamba|' "/dials/modules/dlstbx/src/dlstbx/requirements.py" \
  && python3 /dials/modules/dlstbx/src/dlstbx/requirements.py python-relion -y

CMD ["dials.version"]
