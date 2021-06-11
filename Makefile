DELETE_ON_ERROR:

env:
	python -mvirtualenv env

requirements:
	pip install -r requirements.txt

lint:
	python -m pylint omen2
	black omen2

test:
	rm tests/*_gen.py
	pytest --cov omen2 -v tests

publish:
	rm -rf dist
	python3 setup.py bdist_wheel
	twine upload dist/*

install-hooks:
	pre-commit install
