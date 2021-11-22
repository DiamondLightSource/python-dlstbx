FROM dials/dials:latest AS builder

WORKDIR /dials/modules

ADD . ./dlstbx
RUN source /dials/dials \
  && libtbx.pip install -e ./dlstbx --no-deps \
  && libtbx.python ./dlstbx/src/dlstbx/requirements.py -y \
  && libtbx.refresh

# Copy to final image
FROM centos:7
COPY --from=builder /dials /dials
COPY --from=builder /docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod 0755 /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["dials.version"]
