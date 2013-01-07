from setuptools import setup

setup(
    name='nzbfs',
    version='0.2.0',
    packages=['nzbfs'],
    scripts=['scripts/nzbfs'],
    requires=['xattr', 'yenc'],
)
