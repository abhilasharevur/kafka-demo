#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

setup(
    name='kafka-demo',
    version='1.0',
    description='A useful module',
    author='Abhilasha Revur',
    author_email='abhilasha.m.revur@gmail.com',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['kafka-python', 'psycopg2', 'faker', 'argparse', 'configparser', 'pytest'],
    package_data={
        '': [
            'DemoProducer/auth/ca.pem',
            'DemoProducer/auth/service.cert',
            'DemoProducer/auth/service.key',
            'DemoConsumer/auth/ca.pem',
            'DemoConsumer/auth/service.cert',
            'DemoConsumer/auth/service.key',
            'Postgresql/database.ini'
        ]
    }
)
