DELETE_ON_ERROR:

env:
	python -mvirtualenv env

requirements:
	pip install -r requirements.txt

lint:
	python -m pylint omen2

test:
	pytest --cov notanorm -v tests

publish:
	rm -rf dist
	python3 setup.py bdist_wheel
	twine upload dist/*
