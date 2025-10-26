FROM gcr.io/distroless/static:nonroot
COPY bin/linux_amd64/cloudRunDownloadToGcs /cloudRunDownloadToGcs
USER nonroot:nonroot
ENTRYPOINT ["/cloudRunDownloadToGcs"]
