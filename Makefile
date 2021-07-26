clean:
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	name '*~' -exec rm --force  {}

lint:
	pylint data_comparator/data_comparator.py

run:
	python -m data_comparator.app

test:
	PYTHONPATH=. python -m pytest -s -o log_cli=true --exten "$(filter-out $@,$(MAKECMDGOALS))"
test_all:
	PYTHONPATH=. python -m pytest -s -o log_cli=true
unit_test:
	PYTHONPATH=. python -m pytest -s -o log_cli=true
integration_test:
	PYTHONPATH=. python -m pytest -s -o log_cli=true
smoke_test:
	PYTHONPATH=. python -m pytest -s -o log_cli=true