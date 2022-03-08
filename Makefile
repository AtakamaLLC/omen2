DELETE_ON_ERROR:

env:
	python -mvirtualenv env

requirements:
	pip install -r requirements.txt

lint:
	python -m pylint omen2
	black omen2

docs:
	docmd omen2 -o docs2 -u https://github.com/atakamallc/omen2/blob/master/omen2

black:
	black omen2

test:
	pytest -n=3 --cov omen2 -v tests

publish:
	rm -rf dist
	python3 setup.py bdist_wheel
	twine upload dist/*

install-hooks:
	pre-commit install
