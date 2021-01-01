clean:
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	name '*~' -exec rm --force  {}
lint:
	pylint
run:
	python -m data_comparator.app
test:
	PYTHONPATH=. python -m pytest -s -o log_cli=true