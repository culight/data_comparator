clean:
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	name '*~' -exec rm --force  {}

lint:
	pylint data_comparator/data_comparator.py

run:
	python -m data_comparator.app

.PHONY : test_functional test_unit test_integration

test_functional:
	PYTHONPATH=. python -m pytest -s -m functional -o log_cli=true --exten "$(filter-out $@,$(MAKECMDGOALS))"
test_unit:
	PYTHONPATH=. python -m pytest -s -m unit -o log_cli=true --exten "$(filter-out $@,$(MAKECMDGOALS))"
test_integration:
	PYTHONPATH=. python -m pytest -s -m integration -o log_cli=true --exten "$(filter-out $@,$(MAKECMDGOALS))"
test_smoke:
	PYTHONPATH=. python -m pytest -s -m smoke -o log_cli=true --exten "$(filter-out $@,$(MAKECMDGOALS))"
test:
	PYTHONPATH=. python -m pytest -s -m functional -o log_cli=true --exten "$(filter-out $@,$(MAKECMDGOALS))"
	PYTHONPATH=. python -m pytest -s -m unit -o log_cli=true --exten "$(filter-out $@,$(MAKECMDGOALS))"
	PYTHONPATH=. python -m pytest -s -m integration -o log_cli=true --exten "$(filter-out $@,$(MAKECMDGOALS))"
