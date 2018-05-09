from functools import partial
import os
from setuptools import setup, find_packages

name = 'scaling'  # FIXME: what a terrible name


if __name__ == '__main__':
    setup(
        name=name,
        version="0.1",
        packages=[name],
        install_requires=['attrs', 'ply', 'paramiko', 'toolz', 'sympy', 'toposort', 'regex', 'pytoml'],
        entry_points = {'console_scripts': ['scaling = scaling.__main__:main']}
    )
