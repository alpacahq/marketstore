all:
	python setup.py build && python setup.py install

install: all

unittest: all
	pytest -s -v -q ./pymarketstore/test_client.py
