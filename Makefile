include .envrc

## build/cloudRunDownloadZippedCsvToBigquery: build the cmd/cloudRunDownloadZippedCsvToBigquery application
.PHONY: build/cloudRunDownloadZippedCsvToBigquery
build/cloudRunDownloadZippedCsvToBigquery:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s" -o ./bin/linux_amd64/cloudRunDownloadZippedCsvToBigquery ./cmd/cloudRunDownloadZippedCsvToBigquery

## docker/build/cloudRunDownloadZippedCsvToBigquery/dev: build the cmd/cloudRunDownloadZippedCsvToBigquery application
.PHONY: docker/build/cloudRunDownloadZippedCsvToBigquery/dev
docker/build/cloudRunDownloadZippedCsvToBigquery/dev: build/cloudRunDownloadZippedCsvToBigquery
	docker build --platform=linux/amd64 -t sqlpipe/cloud-run-download-zipped-csv-to-bigquery:dev -f ./cmd/cloudRunDownloadZippedCsvToBigquery/Dockerfile .

## docker/push/cloudRunDownloadZippedCsvToBigquery/dev: push the cmd/cloudRunDownloadZippedCsvToBigquery application
.PHONY: docker/push/cloudRunDownloadZippedCsvToBigquery/dev
docker/push/cloudRunDownloadZippedCsvToBigquery/dev: docker/build/cloudRunDownloadZippedCsvToBigquery/dev
	docker push sqlpipe/cloud-run-download-zipped-csv-to-bigquery:dev

## run/cloudRunDownloadZippedCsvToBigquery/dev: run the cmd/cloudRunDownloadZippedCsvToBigquery application
# .PHONY: run/cloudRunDownloadZippedCsvToBigquery/dev
# run/cloudRunDownloadZippedCsvToBigquery/dev: docker/push/cloudRunDownloadZippedCsvToBigquery/dev
	

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
