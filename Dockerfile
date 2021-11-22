FROM dials/dials:latest AS builder

WORKDIR /dials/modules

ADD . ./dlstbx
RUN source /dials/dials && libtbx.pip install -e ./dlstbx --no-deps
RUN source /dials/dials && libtbx.python ./dlstbx/src/dlstbx/requirements.py -y
RUN source /dials/dials && libtbx.refresh

# Copy to final image
FROM centos:7
COPY --from=builder /dials /dials

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["dials.version"]
