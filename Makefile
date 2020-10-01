

clean-pyc:
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	name '*~' -exec rm --force  {}

lint:
	flake8 --exclude=.tox

run:
	python manage.py runserver

test:
	PYTHONPATH=. pytest -s ./tests