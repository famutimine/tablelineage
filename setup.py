import setuptools
from setuptools import setup, find_packages
import os

from urllib.request import urlopen

with urlopen("https://raw.githubusercontent.com/famutimine/tablelineage/main/README.md") as fh:
    long_description = fh.read().decode()

setuptools.setup(
    name='tablelineage',
    version='0.0.6',
    description='A python library to get unity catalog table lineage information using azure databricks api',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/famutimine/tablelineage',
    author='Daniel Famutimi MD, MPH',
    author_email='danielfamutimi@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering :: Mathematics',
        'Topic :: Scientific/Engineering :: Information Analysis',
    ],
    keywords='descriptive statistics',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'setuptools==65.6.3',
        'pandas==2.2.3',
        'requests==2.32.3'
    ],
)
