#!/usr/bin/python -u

import sys, os, argparse, commands, time
import logging

opencgaHome = "";
rCmd = "";

logging.basicConfig(level=logging.DEBUG)

#Functions
def checkGcsaHome():
    try:
        global opencgaHome
        global rCmd
        #opencgaHome = os.environ["GCSA_HOME"]
        opencgaHome = "/httpd/bioinfo/opencga"
        rCmd = opencgaHome +"/analysis/communities-structure-detection/communities-structure-detection.r"
    except:
        print("Environment variable OPENCGA_HOME is not set")
        sys.exit(-1)


def execute(cmd):
    status, output = commands.getstatusoutput(cmd)
    # print(str(status)+" -> "+output+" - "+cmd)
    logging.info(output+" - "+cmd)

def run(sifnetwork, outdir):
    execute("touch "+outdir+"/a.txt")
    execute("cp "+sifnetwork+" "+outdir)
    time.sleep(10);
    print("done")


parser = argparse.ArgumentParser(prog="communities-structure-detection")
parser.add_argument("-i","--sifnetwork", required=True, help="input SIF file to be analyzed")
parser.add_argument("--outdir", help="output directory")
args = parser.parse_args()


#outdir is the parent file dir by default
if args.outdir is None:
    args.outdir = os.path.abspath(os.path.join(args.input, os.path.pardir))

if os.path.isfile(args.sifnetwork) is False:
    sys.exit(-1)

if os.path.isdir(args.outdir) is False:
    execute("mkdir "+args.outdir)

checkGcsaHome()


#print args.type
#print args.input
#print args.outdir

run(args.sifnetwork, args.outdir)



