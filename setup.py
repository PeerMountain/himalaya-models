#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = ["happybase==1.1.0"]

setup_requirements = ["happybase==1.1.0"]

setup(
    author="Jonatas Baldin",
    author_email='jonatas.baldin@maecenas.co',
    classifiers=[
        'Programming Language :: Python :: 3.6',
    ],
    description="HBase Models for Himalaya",
    install_requires=requirements,
    license="MIT license",
    long_description=readme,
    include_package_data=True,
    keywords='hymalaya_hbase_models',
    name='hymalaya_hbase_models',
    packages=find_packages(include=['hymalaya_hbase_models']),
    setup_requires=setup_requirements,
    test_suite='tests',
    url='',
    version='0.1.0',
    zip_safe=False,
)
