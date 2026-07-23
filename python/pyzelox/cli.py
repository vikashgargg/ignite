import sys

from pyzelox import _native


def main():
    # When the Zelox CLI is invoked via `python -m pyzelox`, the first argument in `sys.argv` is
    # the absolute path to `__main__.py`.
    _native.main(sys.argv)
