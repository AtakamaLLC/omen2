DELETE_ON_ERROR:

env:
	python -mvirtualenv env

requirements:
	python -mpip install -r requirements.txt

lint:
	python -m pylint omen2
	black omen2

docs:
	PYTHONPATH=. docmd omen2 -o docs -u https://github.com/atakamallc/omen2/blob/master/omen2

black:
	black omen2 tests

test:
	pytest -n=3 --cov omen2 -v tests -k "not perf"
	# parallel testing of perf tests doesn't work
	pytest --cov omen2 -v tests -k "perf"

publish:
	rm -rf dist
	python3 setup.py bdist_wheel
	twine upload dist/*

install-hooks:
	pre-commit install


.PHONY: docs black publish env requirements
