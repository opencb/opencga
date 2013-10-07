#!/usr/bin/python -u

import sys, os, argparse, commands

def execute(cmd):
    status, output = commands.getstatusoutput(cmd)
    print(str(status)+" -> "+output+" - "+cmd)



def main(vcf, ped, outdir):

    variantPath = os.path.abspath(os.path.join(os.environ["_"], os.pardir))

    aux_vcf = vcf + ".bier"
    annot_vcf = "annot_" + vcf

    # execute('./variant annot --vcf-file '+vcf+' --control-prefix BIER --control ./controls/bier/bier.gz --outdir '+outdir+' --output-file ' + aux_vcf)
    # execute('./variant annot --vcf-file '+ aux_vcf +' --control-prefix 1000G --control-list ./controls/1000G/list.txt --outdir '+outdir+' --output-file ' + annot_vcf)
    # execute('./variant stats --vcf-file '+ annot_vcf +' --ped-file '+ ped +' --outdir '+ outdir)
    print variantPath+'/variant.sh stats --vcf-file '+ vcf +' --ped-file '+ ped +' --outdir '+ outdir
    execute("export JAVA_HOME=/opt/jdk1.7.0_13 && " variantPath+'/variant.sh stats --vcf-file '+ vcf +' --ped-file '+ ped +' --outdir '+ outdir)
    # execute('../../indexer/indexerManager.py -t vcf -i ' + annot_vcf + '  --outdir ' + outdir )


#variant stats --vcf-file file.vcf --ped-file file.ped --outdir /home/aaleman/tmp/hola
# variant annot --vcf-file file.vcf --control-prefix BIER --control /media/data/controls/bier/bier.gz --outdir /home/aaleman/tmp/hola
#  variant annot --vcf-file file.vcf --control-prefix 1000G --control-list /media/data/controls/1000genomes/list.txt --outdir /home/aaleman/tmp/hola

parser = argparse.ArgumentParser(prog="variant")
parser.add_argument("-v","--vcf-file", required=True, help="input vcf file to be indexed")
parser.add_argument("-p","--ped-file", required=True,  help="input ped file to be indexed")
parser.add_argument("--outdir", help="output directory")
args = parser.parse_args()


#outdir is the parent file dir
if args.outdir is None:
    args.outdir = os.path.abspath(os.path.join(args.input, os.path.pardir))

if os.path.isfile(args.vcf_file) is False:
    sys.exit(-1)

if os.path.isfile(args.ped_file) is False:
    sys.exit(-1)

if os.path.isdir(args.outdir) is False:
    execute("mkdir "+args.outdir)

# print os.getcwd()
main(args.vcf_file, args.ped_file, args.outdir)
