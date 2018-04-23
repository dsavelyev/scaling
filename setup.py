from setuptools import setup, find_packages
import pip
import subprocess

name = 'scaling'  # FIXME: what a terrible name

if __name__ == '__main__':
    subprocess.check_output(['pip', 'install', 'ply'])

    from scaling import exprs as se
    se._generate_tables()

    setup(
        name=name,
        version="0.1",
        py_modules=['scaling'],
        install_requires=['attrs', 'ply', 'paramiko', 'toolz', 'sympy', 'toposort', 'regex', 'pytoml'],
    )
