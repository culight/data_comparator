#!/usr/bin/env python
import setuptools

setuptools.setup(
    name="data-comparator-dmoton3.14",
    version='0.5.7',
    author='Demerrick Moton',
    author_email="dmoton3.14@gmail.com",
    packages={
        "data_comparator",
        "data_comparator.components",
        "data_comparator.ui",
    },
    include_package_data=True,
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    url="https://github.com/culight/data_comparator",
    python_requires='>=3.6',
)
