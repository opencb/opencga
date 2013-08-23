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

import unittest
import random
import gzip
import tabix

EXAMPLEFILE = '../example.gtf.gz'

def load_example_regions(path):
    alldata = []
    for line in gzip.GzipFile(EXAMPLEFILE):
        fields = line.decode('ascii')[:-1].split('\t')
        seqid = fields[0]
        begin = int(fields[3])
        end = int(fields[4])
        alldata.append((seqid, begin, end, fields[:7]))

    return alldata

def does_overlap(A, B, C, D):
    return (A <= D <= B) or (C <= B <= D)

def sample_test_dataset(regions, ntests):
    seqids = [seqid for seqid, _, _, _ in regions]
    lowerbound = max(0, min(begin for _, begin, _, _ in regions) - 1000)
    upperbound = max(end for _, _, end, _ in regions) + 1000

    tests = []
    for i in range(ntests):
        seqid = random.choice(seqids)
        low = random.randrange(lowerbound, upperbound)
        high = random.randrange(low, upperbound)

        # for 1-based both-end inclusive intervals
        matches = [info for seq, begin, end, info in regions
                   if seqid == seq and does_overlap(begin, end, low, high)]

        tests.append((seqid, low, high, matches))

    return tests

def tbresult2excerpt(tbmatches):
    return [fields[:7] for fields in tbmatches]

class TabixTest(unittest.TestCase):
    regions = load_example_regions(EXAMPLEFILE)
    testset = sample_test_dataset(regions, 500)

    def setUp(self):
        self.tb = tabix.Tabix(EXAMPLEFILE)

    def testQuery(self):
        for seqid, low, high, matches in self.testset:
            tbresult = tbresult2excerpt(self.tb.query(seqid, low, high))
            self.assertEqual(tbresult, matches)

    def testQueryS(self):
        for seqid, low, high, matches in self.testset:
            tbresult = tbresult2excerpt(self.tb.querys('%s:%d-%d' %
                                                       (seqid, low, high)))
            self.assertEqual(tbresult, matches)


if __name__ == '__main__':
    unittest.main()
