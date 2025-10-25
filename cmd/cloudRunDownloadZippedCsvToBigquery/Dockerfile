FROM gcr.io/distroless/static:nonroot
COPY bin/linux_amd64/cloudRunDownloadZippedCsvToBigquery /cloudRunDownloadZippedCsvToBigquery
USER nonroot:nonroot
ENTRYPOINT ["/cloudRunDownloadZippedCsvToBigquery"]
