import mincemeat
from setuptools import setup

setup(
    name = "mincemeat",
    version = mincemeat.VERSION,
    description = ("Fork of mincemeat.py lightweight Python Map-Reduce framework http://remembersaurus.com/mincemeatpy/"),
    license = "MIT License",
    url = "https://github.com/pjkundert/mincemeatpy",
    py_modules = ['mincemeat'],
    classifiers = [
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.5",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
    ],
)

