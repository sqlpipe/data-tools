include .envrc

## docker/push/cloudRunDownloadToGcs/dev: push the cmd/cloudRunDownloadToGcs application
.PHONY: docker/push/cloudRunDownloadToGcs/dev
docker/push/cloudRunDownloadToGcs/dev:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s" -o ./bin/linux_amd64/cloudRunDownloadToGcs ./cmd/cloudRunDownloadToGcs
	TS=$$(date +%s); \
		echo "TS: $$TS"; \
		docker build --platform=linux/amd64 -t sqlpipe/cloud-run-download-to-gcs:dev-$$TS -f ./cmd/cloudRunDownloadToGcs/Dockerfile . && \
		docker push sqlpipe/cloud-run-download-to-gcs:dev-$$TS && \
		curl -k -X POST $(AIRFLOW_URL)/api/v1/variables -d "{\"key\":\"cloud-run-download-to-gcs-dev-tag\",\"value\":\"dev-$$TS\"}" $(CURL_AUTH_FLAGS) -H "Content-Type: application/json"	

## docker/push/cloudRunDownloadToGcs/prod: push the cmd/cloudRunDownloadToGcs application
.PHONY: docker/push/cloudRunDownloadToGcs/prod
docker/push/cloudRunDownloadToGcs/prod:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s" -o ./bin/linux_amd64/cloudRunDownloadToGcs ./cmd/cloudRunDownloadToGcs
	
	version=1; \
		docker build --platform=linux/amd64 -t sqlpipe/cloud-run-download-to-gcs:$$version -f ./cmd/cloudRunDownloadToGcs/Dockerfile . && \
		docker push sqlpipe/cloud-run-download-to-gcs:$$version

## docker/push/cloudRunUnzipInGcs/dev: push the cmd/cloudRunUnzipInGcs application
.PHONY: docker/push/cloudRunUnzipInGcs/dev
docker/push/cloudRunUnzipInGcs/dev:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s" -o ./bin/linux_amd64/cloudRunUnzipInGcs ./cmd/cloudRunUnzipInGcs
	TS=$$(date +%s); \
		echo "TS: $$TS"; \
		docker build --platform=linux/amd64 -t sqlpipe/cloud-run-unzip-in-gcs:dev-$$TS -f ./cmd/cloudRunUnzipInGcs/Dockerfile . && \
		docker push sqlpipe/cloud-run-unzip-in-gcs:dev-$$TS && \
		curl -k -X POST $(AIRFLOW_URL)/api/v1/variables -d "{\"key\":\"cloud-run-unzip-in-gcs-dev-tag\",\"value\":\"dev-$$TS\"}" $(CURL_AUTH_FLAGS) -H "Content-Type: application/json"	

## docker/push/cloudRunUnzipInGcs/prod: push the cmd/cloudRunUnzipInGcs application
.PHONY: docker/push/cloudRunUnzipInGcs/prod
docker/push/cloudRunUnzipInGcs/prod:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s" -o ./bin/linux_amd64/cloudRunUnzipInGcs ./cmd/cloudRunUnzipInGcs
	version=1; \
		docker build --platform=linux/amd64 -t sqlpipe/cloud-run-unzip-in-gcs:$$version -f ./cmd/cloudRunUnzipInGcs/Dockerfile . && \
		docker push sqlpipe/cloud-run-unzip-in-gcs:$$version

## docker/push/cloudRunRemoveValuesFromCsv/dev: push the cmd/cloudRunRemoveValuesFromCsv application
.PHONY: docker/push/cloudRunRemoveValuesFromCsv/dev
docker/push/cloudRunRemoveValuesFromCsv/dev:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s" -o ./bin/linux_amd64/cloudRunRemoveValuesFromCsv ./cmd/cloudRunRemoveValuesFromCsv
	TS=$$(date +%s); \
		echo "TS: $$TS"; \
		docker build --platform=linux/amd64 -t sqlpipe/cloud-run-remove-values-from-csv:dev-$$TS -f ./cmd/cloudRunRemoveValuesFromCsv/Dockerfile . && \
		docker push sqlpipe/cloud-run-remove-values-from-csv:dev-$$TS && \
		curl -k -X POST $(AIRFLOW_URL)/api/v1/variables -d "{\"key\":\"cloud-run-remove-values-from-csv-dev-tag\",\"value\":\"dev-$$TS\"}" $(CURL_AUTH_FLAGS) -H "Content-Type: application/json"

## docker/push/cloudRunRemoveValuesFromCsv/prod: push the cmd/cloudRunRemoveValuesFromCsv application
.PHONY: docker/push/cloudRunRemoveValuesFromCsv/prod
docker/push/cloudRunRemoveValuesFromCsv/prod:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s" -o ./bin/linux_amd64/cloudRunRemoveValuesFromCsv ./cmd/cloudRunRemoveValuesFromCsv
	version=1; \
		docker build --platform=linux/amd64 -t sqlpipe/cloud-run-remove-values-from-csv:$$version -f ./cmd/cloudRunRemoveValuesFromCsv/Dockerfile . && \
		docker push sqlpipe/cloud-run-remove-values-from-csv:$$version

## tidy: format all .go files, and tidy and vendor module dependencies
.PHONY: tidy
tidy:
	@echo 'Tidying module dependencies...'
	go mod tidy
	@echo 'Verifying and vendoring module dependencies...'
	go mod verify
	go mod vendor
	@echo 'Formatting .go files...'
	go fmt ./...

## audit: run quality control checks
.PHONY: audit
audit:
	@echo 'Checking module dependencies...'
	go mod tidy -diff
	go mod verify
	@echo 'Vetting code...'
	go vet ./...
	go tool staticcheck ./...
	@echo 'Running tests...'
	go test -race -vet=off ./...
