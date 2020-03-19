vet:
	go vet ./...

lint:
	golint ./..

test: unit-test acceptance-test

unit-test:
	go test --count=1 -v .

acceptance-test:
	docker-compose -f tests/docker-compose.yml build --no-cache
	docker-compose -f tests/docker-compose.yml up -d
	go test --count=1 -v ./tests || (sleep 2; docker-compose -f tests/docker-compose.yml logs; docker-compose -f tests/docker-compose.yml stop; exit 1)
	docker-compose -f tests/docker-compose.yml stop

deps:
	go get -t -v ./...

ci: deps vet lint test
