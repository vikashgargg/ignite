import sys

from pyzelox import _native


def main():
    # When the Sail CLI is invoked via `python -m pyzelox`, the first argument in `sys.argv` is
    # the absolute path to `__main__.py`.
    _native.main(sys.argv)
