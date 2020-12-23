#!/usr/bin/env python

from distutils.core import setup

setup(
    version='0.5.0',
    author='Demerrick Moton',
    packages=[
        'data_comparator',
        'data_comparator.components'
    ],
    long_description=open('README.md').read(),
)
