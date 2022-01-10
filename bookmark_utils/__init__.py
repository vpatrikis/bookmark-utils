# -*- coding: utf-8 -*-

from ._version import __version__

__short_description__ = "This project is created to perform bookmark operation in python shell"
__author__ = "Srinivasarao Daruna"
__author_email__ = "daruns@amazon.com"

# API
try:
    from .bookmark_for_python_shell import BookMarks
except ImportError: # pragma: no cover
    pass
except: # pragma: no cover
    raise
