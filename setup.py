#!/usr/bin/env python

from nzbfs import __version__

from setuptools import setup

setup(
    name='nzbfs',
    version=__version__,
    packages=['nzbfs'],
    scripts=['scripts/nzbfs'],
    requires=['xattr', 'yenc', 'google.protobuf'],
)
