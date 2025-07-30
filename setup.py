#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name="tap-netsuite-general-ledger",
    version="0.1.0",
    description="Singer.io tap for extracting NetSuite GL Detail data",
    author="Your Name",
    url="https://github.com/ModernAnimal/tap-netsuite-general-ledger",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "singer-python>=5.0.0",
        "aiohttp>=3.8.0",
    ],
    entry_points="""
    [console_scripts]
    tap-netsuite-general-ledger=tap_netsuite_general_ledger:main
    """,
    packages=find_packages(),
    package_data={
        "tap_netsuite_general_ledger": ["schemas/*.json"]
    },
    include_package_data=True,
)