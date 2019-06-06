build:
	docker-compose build tests

test: build
	docker-compose run --rm tests test --cov=jticker_aggregator --cov-report=term-missing

test-watch: build
	docker-compose run --rm tests ptw

lint: build
	docker-compose run --rm --entrypoint pylama tests
	docker-compose run --rm --entrypoint "mypy jticker_aggregator" tests

