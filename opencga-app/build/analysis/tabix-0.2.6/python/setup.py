#!/usr/bin/env python
#
# The MIT License
#
# Copyright (c) 2011 Seoul National University.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Contact: Hyeshik Chang <hyeshik@snu.ac.kr>

from distutils.core import setup, Extension

# Change this to True when you need the knetfile support.
USE_KNETFILE = False

TABIX_SOURCE_FILES = [
    '../bgzf.c', '../bgzip.c', '../index.c', '../knetfile.c', '../kstring.c'
]

define_options = [('_FILE_OFFSET_BITS', 64)]
if USE_KNETFILE:
    define_options.append(('_USE_KNETFILE', 1))

ext_modules = [Extension("tabix", ["tabixmodule.c"] + TABIX_SOURCE_FILES,
                         include_dirs=['..'],
                         libraries=['z'],
                         define_macros=define_options)]

setup (name = 'tabix',
       version = '1.0',
       description = 'Python interface to tabix, a generic indexer '
                     'for TAB-delimited genome position files',
       author = 'Hyeshik Chang',
       author_email = 'hyeshik@snu.ac.kr',
       license = 'MIT',
       ext_modules = ext_modules
)
