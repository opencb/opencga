import argparse
from variant_walker import VariantWalker

class Echo(VariantWalker):
    def setup(self, *arg):
        pass

    def header(self, header):
        for line in header:
            self.write(line)

    def map(self, line):
        self.write(line)
        pass

    def cleanup(self):
        pass

class Cut(VariantWalker):
    def setup(self, *args):
        parser = argparse.ArgumentParser()
        parser.add_argument('--length', default=10, help='The length to trim each line to.')
        args = parser.parse_args(args)
        self.length = int(args.length)

    def header(self, header):
        # Print last line from header
        self.write(header[-1])
        pass

    def map(self, line):
        self.write(line[:self.length])

    def cleanup(self):
        pass

class Simplify(VariantWalker):
    def setup(self, *args):
        pass

    def header(self, header):
        # Print last line from header
        self.write(header[-1])

    def map(self, line):
        # Split line by tab
        fields = line.split('\t')
        # Write fields 0, 1, 3, 4 joined by ':'
        self.write(':'.join([fields[0], fields[1], fields[3], fields[4]]))

    def cleanup(self):
        pass
