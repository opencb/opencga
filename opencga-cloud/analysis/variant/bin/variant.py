#!/usr/bin/python -u

import sys, os, argparse, commands


def execute(cmd):
    status, output = commands.getstatusoutput(cmd)
    print(str(status) + " -> " + output + " - " + cmd)


def main(vcf, ped, outdir):
    variantPath = os.path.abspath(os.path.join(os.environ["_"], os.pardir))

    aux_vcf = ".bier"
    annot_vcf = "annot_" + vcf

    db_file = os.path.basename(vcf)
    db_name = os.path.splitext(db_file)[0] + ".db"

    cmd = "export JAVA_HOME=/opt/jdk1.7.0_40 && " + variantPath + '/variant.sh annot --vcf-file ' + vcf + ' --control-prefix BIER --control /httpd/bioinfo/controls/BIER/bier.vcf.gz --outdir ' + outdir + ' --output-file annot_BIER.vcf'
    print cmd
    execute(cmd)
    cmd = "export JAVA_HOME=/opt/jdk1.7.0_40 && " + variantPath + '/variant.sh annot --vcf-file ' + outdir + '/annot_BIER.vcf' + ' --control-prefix 1000G --control-list /httpd/bioinfo/controls/1000G/list.txt --outdir ' + outdir + ' --output-file annot_final.vcf'
    print cmd
    execute(cmd)

    cmd = "export JAVA_HOME=/opt/jdk1.7.0_40 && " + variantPath + '/variant.sh stats --vcf-file ' + outdir + '/annot_final.vcf' + ' --ped-file ' + ped + ' --outdir ' + outdir + ' --output-file ' + db_name
    print cmd
    execute(cmd)


parser = argparse.ArgumentParser(prog="variant")
parser.add_argument("-v", "--vcf-file", required=True, help="input vcf file to be indexed")
parser.add_argument("-p", "--ped-file", required=True, help="input ped file to be indexed")
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
    execute("mkdir " + args.outdir)

# print os.getcwd()
main(args.vcf_file, args.ped_file, args.outdir)
