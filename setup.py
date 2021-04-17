from setuptools import setup

NAME = 'moviesetl'
VERSION = '0.1'

with open('requirements.txt') as f:
    REQUIREMENTS = f.read().splitlines()

setup(
    name=NAME,
    version=VERSION,
    install_requies=REQUIREMENTS,
    description='Data pipeline that ingests and transforms a movies dataset',
    author='Guido Kosloff Gancedo'
)
