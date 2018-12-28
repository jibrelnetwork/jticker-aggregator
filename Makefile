lint:
	pylama
	mypy jticker_aggregator

test:
	pytest --cov jticker_aggregator
