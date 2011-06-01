#!/usr/bin/env python

from distutils.core import setup

setup(name='libcdmi',
      version='0.1.1',
      description='Client library for CDMI services.',
      author='Ilja Livenson',
      author_email='ilja.livenson@gmail.com',
      url='https://github.com/livenson/libcdmi-python',
      license='BSD (3-clause)',
      packages=['libcdmi'],
      package_dir = {'': 'src'}      
     )