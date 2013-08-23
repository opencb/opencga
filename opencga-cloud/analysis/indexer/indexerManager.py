#!/usr/bin/python -u

import sys, os, argparse, commands

opencgaHome = "";
samtoolsCmd = "";
hpgbamCmd = "";
hpgvarvcfCmd = "";
hpgvarvcfConfig = "";
tabixCmd = "";
tabixbgzipCmd = "";

#Functions
def checkGcsaHome():
    try:
        global opencgaHome
        global samtoolsCmd
        global hpgbamCmd
        global hpgvarvcfCmd
        global hpgvarvcfConfig
        global tabixCmd
        global tabixbgzipCmd
        #opencgaHome = os.environ["GCSA_HOME"]
        opencgaHome = "/httpd/bioinfo/opencga"
        samtoolsCmd = opencgaHome +"/analysis/samtools/samtools"
        hpgbamCmd = opencgaHome +"/analysis/hpg-bam/hpg-bam"
        hpgvarvcfCmd = opencgaHome +"/analysis/hpg-variant/bin/hpg-var-vcf"
        hpgvarvcfConfig = opencgaHome +"/analysis/hpg-variant/bin"
        tabixCmd = opencgaHome +"/analysis/tabix/tabix"
        tabixbgzipCmd = opencgaHome +"/analysis/tabix/bgzip"
        #print(tabixCmd)
        #print(tabixbgzipCmd)
    except:
        print("Environment variable OPENCGA_HOME is not set")
        sys.exit(-1)


def execute(cmd):
    status, output = commands.getstatusoutput(cmd)
    print(str(status)+" -> "+output+" - "+cmd)
    if 35584 == status and "[bam_header_read]" in output:
        sys.exit(-1)
    #if "open: No such file or directory" in output:
        #sys.exit(-1)
    #if 256 == status and "mv" in output:
        #sys.exit(-1)
    #if 512 == status and "tar" in output:
        #sys.exit(-1)
    #if 512 == status and "sort" in output:
        #sys.exit(-1)
    #if 1 == status and "[bgzip]" in output:
        #sys.exit(-1)
    #if 1 == status and "[tabix]" in output:
        #sys.exit(-1)



#INDEXERS
def indexBAM(inputBAM, outdir):
    print("indexing bam file...")
    sortBam(inputBAM, outdir)
    execute(samtoolsCmd + " index " + inputBAM)
    execute("mv "+inputBAM+".bai "+outdir)

    #/hpg-bam stats -b HG00096.chrom20.ILLUMINA.bwa.GBR.exome.20111114.bam -o ./ --db
    execute(hpgbamCmd + " stats " + " -b "+ inputBAM + " -o " + outdir + " --db" )
    print("index complete!")

def sortBam(inputBAM, outdir):
    print("sorting bam file...")
    sortedBam = inputBAM + ".sort.bam"
    sortCmd = samtoolsCmd + " sort " + inputBAM + " " + inputBAM + ".sort" +  " -o "+ outdir
    execute(sortCmd)
    execute("mv "+ sortedBam + " " + inputBAM)
    print("sort complete!")

def indexVCF(inputVCF, outdir):
    #file inputFVCF "gzip compressed"  zcat file |
    inputVCFname = os.path.basename(inputVCF)
    destName = outdir +"/"+ inputVCFname
    print("indexing vcf file...")
    execute("tar -zcvf "+ inputVCF + ".tar.gz" + " " + inputVCF)
    execute("mv "+inputVCF + ".tar.gz "+outdir)
    execute("sort -k1,1 -k2,2n "+ inputVCF + " > " + inputVCF + ".sort.vcf");
    execute(tabixbgzipCmd + " -c " + inputVCF + ".sort.vcf" + " > " + destName + ".gz")
    execute(tabixCmd + " -p vcf " + destName + ".gz")
    execute("rm -rf "+ inputVCF + ".sort.vcf")
    execute(hpgvarvcfCmd + " stats " + " -v "+ inputVCF + " --outdir " + outdir + " --db --config "+hpgvarvcfConfig )
    print("index complete!")



parser = argparse.ArgumentParser(prog="indexer")
parser.add_argument("-i","--input", required=True, help="input file to be indexed")
parser.add_argument("-t","--type", required=True, choices=("bam","vcf"))
parser.add_argument("-c","--compressed", action="count", help="input file is gzipped")
parser.add_argument("--outdir", help="output directory")
args = parser.parse_args()


#outdir is the parent file dir
if args.outdir is None:
    args.outdir = os.path.abspath(os.path.join(args.input, os.path.pardir))

if os.path.isfile(args.input) is False:
    sys.exit(-1)

if os.path.isdir(args.outdir) is False:
    execute("mkdir "+args.outdir)

checkGcsaHome()


#print args.type
#print args.input
#print args.outdir

if args.type == "bam":
    indexBAM(args.input, args.outdir)

elif args.type == "vcf":
    indexVCF(args.input, args.outdir)



