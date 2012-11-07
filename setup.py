from setuptools import setup

setup(
    name='nzbfs',
    version='0.1a',
    packages=['nzbfs'],
    scripts=['scripts/nzbfs'],
    requires=['xattr', 'yenc'],
)
