PYTHON ?= python

.PHONY: install test demo integration-test

install:
	$(PYTHON) -m pip install -r requirements-dev.txt

test:
	$(PYTHON) -m pytest

demo:
	$(PYTHON) scripts/run_demo.py

integration-test:
	$(PYTHON) -m pytest pipeline/test.py -q
