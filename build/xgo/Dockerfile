FROM techknowlogick/xgo:go-1.13.x@sha256:5ad8a3a0a6576e50c1d3e797ab8c2e186caa83c3f062cc275e1a25e29ade1204

# Inject the customized build script
ADD build.sh /build.sh
ENV BUILD /build.sh
RUN chmod +x $BUILD

ENTRYPOINT ["/build.sh"]
