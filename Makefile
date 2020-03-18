vet:
	go vet ./...

lint:
	golint ./..

test: unit-test acceptance-test

unit-test:
	go test --count=1 -v .

acceptance-test:
	docker-compose -f _tests/docker-compose.yml up -d
	go test --count=1 -v ./_tests || (docker-compose -f _tests/docker-compose.yml stop; exit 1)
	docker-compose -f _tests/docker-compose.yml stop

deps:
	go get -u ./...

ci: deps vet lint test
