#!/usr/bin/env python
import setuptools

setuptools.setup(
    name="data-comparator",
    version="0.5.1",
    author="Demerrick Moton",
    author_email="dmoton3.14@gmail.com",
    packages={"data_comparator", "data_comparator.components", "data_comparator.ui",},
    include_package_data=True,
    package_data={
        "data_comparator.components": ["validations_config.json"],
        "data_comparator.ui": [
            "*.ui",
        ],
    },
    description="Data profiling tool with a focus on dataset comparisons",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    url="https://github.com/culight/data_comparator",
    python_requires=">=3.6",
)
