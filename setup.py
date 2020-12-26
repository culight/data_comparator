#!/usr/bin/env python

from distutils.core import setup

setup(
    version='0.5.0',
    author='Demerrick Moton',
    email="dmoton3.14@gmail.com",
    packages=[
        'data_comparator',
        'data_comparator.components'
    ],
    long_description=open('README.md').read(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    url="https://github.com/culight/data_comparator",
    python_requires='>=3.6',
)
