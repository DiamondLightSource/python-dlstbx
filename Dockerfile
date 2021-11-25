FROM dials/dials:latest

WORKDIR /dials/modules

ADD . ./dlstbx
RUN source /dials/dials \
  && libtbx.pip install -e ./dlstbx --no-deps \
  && libtbx.python ./dlstbx/src/dlstbx/requirements.py -y \
  && libtbx.refresh

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["dials.version"]
