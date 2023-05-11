#!/usr/bin/env python

from setuptools import setup

setup(
    name="boto_addins",
    version="0.9.10",
    author="Uploadcare",
    author_email="ak@uploadcare.com",
    description="Async proxy libraries for AWS services.",
    install_requires=[
        'tornado>=6.0.0',
        'botocore>=1.13.0',
        'boto',
        'YURL',
    ],
    keywords="aws amazon S3 storage "
             "lambda request-response invoke",
    url="http://packages.python.org/an_example_pypi_project",
    packages=['boto_addins'],
)
