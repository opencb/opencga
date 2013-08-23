#!/usr/bin/env python

# Author: Heng Li and Aaron Quinlan
# License: MIT/X11

import sys
from ctypes import *
from ctypes.util import find_library
import glob, platform

def load_shared_library(lib, _path='.', ver='*'):
    """Search for and load the tabix library. The
    expectation is that the library is located in
    the current directory (ie. "./")
    """
    # find from the system path
    path = find_library(lib)
    if (path == None): # if fail, search in the custom directory
        s = platform.system()
        if (s == 'Darwin'): suf = ver+'.dylib'
        elif (s == 'Linux'): suf = '.so'+ver
        candidates = glob.glob(_path+'/lib'+lib+suf);
        if (len(candidates) == 1): path = candidates[0]
        else: return None
    cdll.LoadLibrary(path)
    return CDLL(path)

def tabix_init():
    """Initialize and return a tabix reader object
    for subsequent tabix_get() calls.  
    """
    tabix = load_shared_library('tabix')
    if (tabix == None): return None
    tabix.ti_read.restype = c_char_p
    # on Mac OS X 10.6, the following declarations are required.
    tabix.ti_open.restype = c_void_p
    tabix.ti_querys.argtypes = [c_void_p, c_char_p]
    tabix.ti_querys.restype = c_void_p
    tabix.ti_query.argtypes = [c_void_p, c_char_p, c_int, c_int]
    tabix.ti_query.restype = c_void_p
    tabix.ti_read.argtypes = [c_void_p, c_void_p, c_void_p]
    tabix.ti_iter_destroy.argtypes = [c_void_p]
    tabix.ti_close.argtypes = [c_void_p]
    # FIXME: explicit declarations for APIs not used in this script
    return tabix

# OOP interface
class Tabix:
    def __init__(self, fn, fnidx=0):
        self.tabix = tabix_init();
        if (self.tabix == None):
            sys.stderr.write("[Tabix] Please make sure the shared library is compiled and available.\n")
            return
        self.fp = self.tabix.ti_open(fn, fnidx);

    def __del__(self):
        if (self.tabix): self.tabix.ti_close(self.fp)

    def fetch(self, chr, start=-1, end=-1):
        """Generator function that will yield each interval
        within the requested range from the requested file.
        """
        if (self.tabix == None): return
        if (start < 0): iter = self.tabix.ti_querys(self.fp, chr) # chr looks like: "chr2:1,000-2,000" or "chr2"
        else: iter = self.tabix.ti_query(self.fp, chr, start, end) # chr must be a sequence name
        if (iter == None):        
            sys.stderr.write("[Tabix] Malformatted query or wrong sequence name.\n")
            return
        while (1): # iterate
            s = self.tabix.ti_read(self.fp, iter, 0)
            if (s == None): break
            yield s   
        self.tabix.ti_iter_destroy(iter)

# command-line interface
def main():
    if (len(sys.argv) < 3):
        sys.stderr.write("Usage: tabix.py <in.gz> <reg>\n")
        sys.exit(1)
    
    # report the features in the requested interval
    tabix = Tabix(sys.argv[1])
    for line in tabix.fetch(sys.argv[2]):
        print line

if __name__ == '__main__':
    main()
