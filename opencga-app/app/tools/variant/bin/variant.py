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

    cmd_ped = ""
    if  not ped is None:
        cmd_ped = " --ped-file " + ped + " "

    cmd = "export JAVA_HOME=/opt/jdk1.7.0_40 && " + variantPath + '/variant.sh --annot --vcf-file ' + vcf + ' --annot-control-prefix BIER --annot-control-file /httpd/bioinfo/controls/BIER/bier.vcf.gz --outdir ' + outdir + ' --output-file annot_BIER.vcf'
    print cmd
    execute(cmd)

    cmd = "export JAVA_HOME=/opt/jdk1.7.0_40 && " + variantPath + '/variant.sh --annot --vcf-file ' + outdir + '/annot_BIER.vcf' +  ' --annot-control-prefix EVS --annot-control-evs /httpd/bioinfo/controls/EVS/evs.vcf.gz --outdir ' + outdir + ' --output-file annot_BIER_EVS.vcf'
    print cmd
    execute(cmd)

    cmd = "export JAVA_HOME=/opt/jdk1.7.0_40 && " + variantPath + '/variant.sh --annot --vcf-file ' + outdir + '/annot_BIER_EVS.vcf' + ' --annot-control-prefix 1000G --annot-control-list /httpd/bioinfo/controls/1000G/list.txt --outdir ' + outdir + ' --output-file annot_final.vcf'
    print cmd
    execute(cmd)

    cmd = "export JAVA_HOME=/opt/jdk1.7.0_40 && " + variantPath + '/variant.sh --index --annot --annot --stats --annot-snp --vcf-file ' + outdir + '/annot_final.vcf ' + cmd_ped + ' --outdir ' + outdir + ' --output-file ' + db_name
    print cmd
    execute(cmd)


parser = argparse.ArgumentParser(prog="variant")
parser.add_argument("-v", "--vcf-file", required=True, help="input vcf file to be indexed")
parser.add_argument("-p", "--ped-file", required=False, help="input ped file to be indexed")
parser.add_argument("--outdir", help="output directory")
args = parser.parse_args()


#outdir is the parent file dir
if args.outdir is None:
    args.outdir = os.path.abspath(os.path.join(args.input, os.path.pardir))

if os.path.isfile(args.vcf_file) is False:
    sys.exit(-1)

ped = None
if not args.ped_file is None:
    ped = args.ped_file

if not args.ped_file is None and os.path.isfile(args.ped_file) is False:
    sys.exit(-1)

if os.path.isdir(args.outdir) is False:
    execute("mkdir " + args.outdir)

# print os.getcwd()
main(args.vcf_file, ped, args.outdir)
