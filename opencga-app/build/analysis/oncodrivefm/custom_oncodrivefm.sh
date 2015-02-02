#!/usr/bin/env python

import argparse
import sys
import os
import time
from lib.ResultXML import ResultXML


def main():

    # define arguments
    help = argparse.ArgumentDefaultsHelpFormatter(prog=sys.argv[0],max_help_position=70,width=200)
    parser = argparse.ArgumentParser(description='Custom OncodriveFM',formatter_class=lambda prog: help, )

    # Germline parameters
    parser.add_argument('--vcf', type=str, help='VCF file',required=True)
    parser.add_argument('--outdir', type=str, help='Output folder',required=True)
    parser.add_argument('--gver', type=str, help='Human genome release (hg19, hg20) [hg19]',required=False, default="hg19")
    parser.add_argument('--annovar', type=str, help='Annovar path',required=False, default=os.getenv('ANNOVAR_PATH',""))
    parser.add_argument('--estimator', type=str, help='Global score estimator (mean, median) [median]',required=False, default="median")
    parser.add_argument('--gt', type=int, help='Minimum number of mutations per gene [3]',required=False, default=3)

    # PARSE
    args = parser.parse_args()

    if args.gver not in ["hg19","hg20"]:
        print "ERROR: Incorrect human genome release must be hg19 or hg20"
        exit()

    if args.estimator not in ["median","mean"]:
        print "ERROR: Incorrect estimator"
        exit()

    if args.annovar=="":
        print "ERROR: Annovar path not found (it can be specified by ANNOVAR_PATH environment variable)"
        exit()

    # run
    ofm = OncodriveFM(args.vcf,args.estimator,args.gt,args.gver,args.annovar,args.outdir)
    ofm.run()


class OncodriveFM:

    def __init__(self,vcf,estimator,gt,gver,annovar,outdir):
        print "VCF: " + vcf
        print "estimator: " + estimator
        print "gt: " + str(gt)
        print "gver: " + gver
        print "annovar: " + annovar
        print "outdir: " + outdir
        self.vcf = vcf
        self.estimator = estimator
        self.gt = gt
        self.gver = gver
        if self.gver == "hg20": self.gver = "hg38"
        self.annovar = annovar
        self.outdir = outdir


    def run(self):

        if os.path.exists(self.outdir):
            print "WARNING: Output directory already exists"
        else:
            os.mkdir(self.outdir)

        self.vcf_prefix = os.path.basename(self.vcf).replace(".vcf","").replace(".gz","")

        print "Annotating effects..."
        os.system(self.annovar +"/table_annovar.pl " + self.vcf + " " + self.annovar + "/humandb/ -buildver " + self.gver + " -out " + self.outdir + "/" + self.vcf_prefix + " -remove -protocol refGene,ensGene,ljb26_all -operation g,g,f -nastring . -vcfinput > " + self.outdir + "/annovar.o 2> " + self.outdir + "/annovar.e")
        os.system("echo 'SAMPLE\tGENE\tHGNC\tSIFT\tPPH2\tMA' > " + self.outdir + "/" + self.vcf_prefix + ".tdm")
        os.system("cat " + self.outdir + "/" + self.vcf_prefix + "." + self.gver + "_multianno.vcf | grep -v \"Func.ensGene=intergenic\" | sed 's/Gene.refGene/Gene_refGene/g' | sed 's/Gene.ensGene/Gene_ensGene/g' | bcftools query -f \"%INFO/Gene_ensGene\t%INFO/Gene_refGene\t%SIFT_score\t%Polyphen2_HDIV_score\t%MutationAssessor_score[\t%SAMPLE=%GT]\n\" - | awk -F'\t' '{OFS=\"\t\"; for(i=6; i<=NF; i++) { split($i,a,\"=\"); if(a[2]==\"0/1\" || a[2]==\"1/1\") print a[1],$1,$2,$3,$4,$5}}' >> " + self.outdir + "/" + self.vcf_prefix + ".tdm")

        print "Running OncodriveFM..."
        os.system("oncodrivefm --save-data --save-analysis -e median --gt " + str(self.gt) + " -s SIFT,PPH2,MA -o " + self.outdir + " " + self.outdir + "/" + self.vcf_prefix + ".tdm > " + self.outdir + "/oncodrivefm.o 2> " + self.outdir + "/oncodrivefm.e")

        print "Checking significant results..."
        os.system("cut -f 2,3 " + self.outdir + "/" + self.vcf_prefix + ".tdm | grep -v ^GENE | sort -u > " + self.outdir + "/gene_annot.txt")
        os.system("LC_NUMERIC='en_US.UTF-8' sort -k 2 -g " + self.outdir + "/" + self.vcf_prefix + "-genes.tsv | grep -v '#' > " + self.outdir + "/" + self.vcf_prefix + "-genes__sorted.tsv")
        self.formatResults()

        # RESULTS.XML
        rxml = ResultXML("oncodrivefm")

        rxml.addMetaDataItem("date","Date",time.strftime("%c"))

        rxml.addInputItem("vcf","VCF file",self.vcf)
        rxml.addInputItem("estimator","Score estimator",self.estimator)
        rxml.addInputItem("gt","Minimum number of mutations per gene",self.gt)
        rxml.addInputItem("gver","Human genome release",self.gver)
        rxml.addInputItem("outdir","Output directory",self.outdir)

        rxml.addOutputItem("vcf","VCF file","MESSAGE",os.path.basename(self.vcf),"INPUT_PARAM","Input parameters")
        rxml.addOutputItem("estimator","Score estimator","MESSAGE",self.estimator,"INPUT_PARAM","Input parameters")
        rxml.addOutputItem("gt","Minimum number of mutations per gene","MESSAGE",self.gt,"INPUT_PARAM","Input parameters")
        rxml.addOutputItem("gver","Human genome release","MESSAGE",self.gver,"INPUT_PARAM","Input parameters")

        if self.nsig>0:
            rxml.addOutputItem("sig_count","Significant genes","MESSAGE","Found " + str(self.nsig) + " significant genes","","Output results")
        else:
            rxml.addOutputItem("sig_count","Significant genes","MESSAGE","No significant genes found","","Output results")

        rxml.addOutputItem("output_genes","Gene results","FILE",self.vcf_prefix + "-genes__sorted_formatted.tsv","TABLE,ONCODRIVEFM-GENES","Output results")
        rxml.addOutputItem("fatiscan_redirection","Redirect ranked genes to Gene Set Enrichment","TEXT","","REDIRECTION(fatiscan.redirection:Send ranked genes to Gene Set Enrichment...)","Continue processing")
        rxml.addOutputItem("network_miner_redirection","Redirect ranked genes to Gene Set Network Enrichment","TEXT","","REDIRECTION(network_miner.redirection:Send ranked genes to Gene Set Network Enrichment...)","Continue processing")
        if self.nsig>0:
            rxml.addOutputItem("fatigo_redirection","Redirect significant genes to Single Enrichment","TEXT","","REDIRECTION(fatigo.redirection:Send to Single Enrichment...)","Continue processing")
            rxml.addOutputItem("snow_redirection","Redirect significant genes to Network Enrichment","TEXT","","REDIRECTION(snow.redirection:Send to Network Enrichment...)","Continue processing")
            rxml.addOutputItem("protein_viewer_significant","Significant genes network","FILE","significant_genes.txt","PROTEIN_VIEWER,hsapiens","Output results")

        rxml.save(self.outdir + "/result.xml")


    def formatResults(self):

        # load annotations
        annots = {}
        fannots = open(self.outdir + "/gene_annot.txt","r")
        for annot in fannots:
            fields = annot.split("\t")
            annots[fields[0]] = fields[1].strip()
        fannots.close()

        # reformat results
        fin = open(self.outdir + "/" + self.vcf_prefix + "-genes__sorted.tsv","r")
        fout = open(self.outdir + "/" + self.vcf_prefix + "-genes__sorted_formatted.tsv","w")
        rank = open(self.outdir + "/gene_rank.txt","w")
        sig_genes = open(self.outdir + "/significant_genes.txt","w")
        self.nsig = 0

        for line in fin:
            if line.find("#")==0:
                fout.write(line)
            else:
                if line.find("ID")==0:
                    fout.write("hgnc\tensembl_id\tpvalue\tqvalue\n")
                else:
                    fields = line.split("\t")
                    fout.write(annots[fields[0]] + "\t" + line)
                    rank.write(annots[fields[0]] + "\t" + fields[1] + "\n")

                    if float(fields[2])<0.05:
                        sig_genes.write(annots[fields[0]]+"\n")
                        self.nsig = self.nsig + 1

        fin.close()
        fout.close()
        rank.close()
        sig_genes.close()

        fatiscan = open(self.outdir + "/fatiscan.redirection","w")
        fatiscan.write("tool=fatiscan\n")
        fatiscan.write("jobName=untitled\n")
        fatiscan.write("jobDescription=redirected from job $JOB_NAME\n")
        fatiscan.write("inputFile=$JOB_FOLDER/gene_rank.txt\n")
        fatiscan.close()

        network_miner = open(self.outdir + "/network_miner.redirection","w")
        network_miner.write("tool=network-miner\n")
        network_miner.write("jobName=untitled\n")
        network_miner.write("jobDescription=redirected from job $JOB_NAME\n")
        network_miner.write("inputFile=$JOB_FOLDER/gene_rank.txt\n")
        network_miner.write("type=genes\n")
        network_miner.close()

        if self.nsig>0:

            fatigo = open(self.outdir + "/fatigo.redirection","w")
            fatigo.write("tool=fatigo\n")
            fatigo.write("jobName=untitled\n")
            fatigo.write("jobDescription=redirected from job $JOB_NAME\n")
            fatigo.write("inputFile=$JOB_FOLDER/significant_genes.txt\n")
            fatigo.write("comparisonRadio=list2genome\n")
            fatigo.close()

            snow = open(self.outdir + "/snow.redirection","w")
            snow.write("tool=snow\n")
            snow.write("jobName=untitled\n")
            snow.write("jobDescription=redirected from job $JOB_NAME\n")
            snow.write("inputFile=$JOB_FOLDER/significant_genes.txt\n")
            snow.write("type=genes\n")
            snow.close()




#
# <?xml version="1.0" encoding="UTF-8"?>
# <result>
# 	<metadata>
# 		<item name="version" title="SDK version" type="MESSAGE" tags="" style="" group="" context="">v0.7</item>
# 		<item name="tool" title="OncodriveFM" type="MESSAGE" tags="" style="" group="" context="">oncodrivefm</item>
# 		<item name="date" title="Job date" type="MESSAGE" tags="" style="" group="" context="">`date`</item>
# 	</metadata>
# 	<input>
# 		<item name="vcf" title="VCF file" type="MESSAGE" tags="" style="" group="" context="">$vcf</item>
# 		<item name="gver" title="Genome version" type="MESSAGE" tags="" style="" group="" context="">$gver</item>
# 		<item name="estimator" title="Estimator" type="MESSAGE" tags="" style="" group="" context="">$estimator</item>
# 		<item name="outdir" title="Output directory" type="MESSAGE" tags="" style="" group="" context="">$outdir</item>
# 	</input>
# 	<output>
# 		<item name="input_vcf" title="Input VCF" type="MESSAGE" tags="INPUT_PARAM" style="" group="Input parameters" context="">`basename $vcf`</item>
# 		<item name="input_gver" title="Genome version" type="MESSAGE" tags="INPUT_PARAM" style="" group="Input parameters" context="">$gver</item>
# 		<item name="input_estimator" title="Estimator" type="MESSAGE" tags="INPUT_PARAM" style="" group="Input parameters" context="">$estimator</item>
# 		<item name="output_genes" title="Significant genes" type="FILE" tags="TABLE,ONCODRIVEFM-GENES" style="" group="Output results" context="">${vcf_prefix}-genes.tsv</item>
# 		<item name="output_pathways" title="Significant KEGG pathways" type="FILE" tags="TABLE,ONCODRIVEFM-PATHWAYS" style="" group="Output results" context="">${vcf_prefix}-pathways.tsv</item>
# 	</output>
# </result>



if __name__ == "__main__":
  main()





