FROM gcr.io/distroless/static:nonroot
COPY bin/linux_amd64/cloudRunUnzipInGcs /cloudRunUnzipInGcs
USER nonroot:nonroot
ENTRYPOINT ["/cloudRunUnzipInGcs"]
