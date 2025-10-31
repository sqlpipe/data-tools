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

## docker/push/cloudRunCsvCleaner/dev: push the cmd/cloudRunCsvCleaner application
.PHONY: docker/push/cloudRunCsvCleaner/dev
docker/push/cloudRunCsvCleaner/dev:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s" -o ./bin/linux_amd64/cloudRunCsvCleaner ./cmd/cloudRunCsvCleaner
	TS=$$(date +%s); \
		echo "TS: $$TS"; \
		docker build --platform=linux/amd64 -t sqlpipe/cloud-run-csv-cleaner:dev-$$TS -f ./cmd/cloudRunCsvCleaner/Dockerfile . && \
		docker push sqlpipe/cloud-run-csv-cleaner:dev-$$TS && \
		curl -k -X POST $(AIRFLOW_URL)/api/v1/variables -d "{\"key\":\"cloud-run-csv-cleaner-dev-tag\",\"value\":\"dev-$$TS\"}" $(CURL_AUTH_FLAGS) -H "Content-Type: application/json"

## docker/push/cloudRunCsvCleaner/prod: push the cmd/cloudRunCsvCleaner application
.PHONY: docker/push/cloudRunCsvCleaner/prod
docker/push/cloudRunCsvCleaner/prod:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s" -o ./bin/linux_amd64/cloudRunCsvCleaner ./cmd/cloudRunCsvCleaner
	version=1; \
		docker build --platform=linux/amd64 -t sqlpipe/cloud-run-csv-cleaner:$$version -f ./cmd/cloudRunCsvCleaner/Dockerfile . && \
		docker push sqlpipe/cloud-run-csv-cleaner:$$version

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
