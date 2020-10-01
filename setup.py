#!/usr/bin/env python

from distutils.core import setup

setup(
    name='0.5.0',
    author='Demerrick Moton',
    author_email="dmoton3.14@gmail.com",
    packages=[
        'data_comparator',
        'data_comparator.components'
    ],
    long_description=open('README.md').read(),
)
