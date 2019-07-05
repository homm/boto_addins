#!/usr/bin/env python

from setuptools import setup

setup(
    name="boto_addins",
    version="0.8.1",
    author="Uploadcare",
    author_email="ak@uploadcare.com",
    description="Async proxy libraries for AWS services.",
    install_requires=[
        'tornado>=4.0.0',
        'tornado-botocore>=1.5.0',
        'boto',
        'YURL',
        'six',
    ],
    keywords="aws amazon S3 SQS messages storage, "
             "lambda request-response invoke",
    url="http://packages.python.org/an_example_pypi_project",
    packages=['boto_addins'],
)
