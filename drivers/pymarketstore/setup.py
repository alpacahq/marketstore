from distutils.core import setup

setup(
    name='pymarketstore',
    version='1.0',
    description='Marketstore python driver',
    author='Rob Schmicker',
    author_email='rschmicker@brightforge.com',
    url='https://github.com/alpacahq/marketstore',
    packages=['pymarketstore',],
    install_requires=[
        'msgpack-python',
        'numpy',
        'requests',
        'six',
        'urllib3',
        'pytest',
    ],
)
