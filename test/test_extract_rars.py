#!/usr/bin/env python

import subprocess
import os

import pytest

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures')


def copy_to_dir(dirpath, filename):
    with open(os.path.join(FIXTURES, filename), 'r') as read_fh:
        with open(os.path.join(dirpath, filename), 'w') as write_fh:
            write_fh.write(read_fh.read())


def test_simple_rar(tmpdir):
    copy_to_dir(tmpdir.strpath, 'simple.rar')
    copy_to_dir(tmpdir.strpath, 'simple.rar-89.nzbfs')
    subprocess.check_call(['nzbfs-extract-rars', tmpdir.strpath])


def test_invalid_rar(tmpdir):
    copy_to_dir(tmpdir.strpath, 'invalid.rar')

    with pytest.raises(Exception):
        subprocess.check_call(['nzbfs-extract-rars', tmpdir.strpath])


if __name__ == '__main__':
    pytest.main()
