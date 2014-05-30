#!/usr/bin/python
import sys, os, argparse, commands, time


def execute(cmd):
    status, output = commands.getstatusoutput(cmd)
    if	status != 0:
        print("EXIT ERROR")
	print(str(status) + " -> " + output + " - " + cmd)
	sys.exit(1)

a = sys.argv
a.append("--out")
a.append("output.test")
del a[0]

orig = " ".join(a)
print orig

parser = argparse.ArgumentParser(prog="variant")
parser.add_argument("--out",  required=False)
parser.add_argument("--outdir", required=False)
args, unknown = parser.parse_known_args()

start_time = time.time()
cmd = "/httpd/bioinfo/opencga/analysis/hpg-variant/bin/hpg-var-gwas " + orig
print cmd
execute(cmd)

end_time = time.time() - start_time

print "GWAS: " + str(end_time)

print args.outdir

out = "output.test"

filename = args.outdir + "/" + out

start_time = time.time()

cmd = "/opt/R/R-2.15.2/bin/R CMD BATCH --vanilla \"--args filename='" + filename +"' current_dir='" + args.outdir + "' \"  /httpd/bioinfo/opencga/analysis/hpg-variant/bin/manhattan.R"
print cmd
execute(cmd)

end_time = time.time() - start_time

print "R: " + str(end_time)
