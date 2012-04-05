#!/usr/bin/env python
from setuptools import setup, find_packages

import querysetunion

CLASSIFIERS = [
    'Intended Audience :: Developers',    
    'Natural Language :: English',
    'Operating System :: OS Independent',
    'Programming Language :: Python'    
]

KEYWORDS = 'django queryset inheritance'

setup(name = 'querysetunion',
    version = querysetunion.__version__,
    description = """Django queryset for multiple models""",
    author = querysetunion.__author__,
    url = "https://github.com/oxys-net/django-querysetunion",
    packages = find_packages(),
    classifiers = CLASSIFIERS,
    keywords = KEYWORDS,
    zip_safe = True,
)
