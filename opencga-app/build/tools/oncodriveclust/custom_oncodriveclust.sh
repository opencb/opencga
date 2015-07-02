#!/usr/bin/env python

import argparse
import sys
import os
import time
from lib.ResultXML import ResultXML

def main():

    # define arguments
    help = argparse.ArgumentDefaultsHelpFormatter(prog=sys.argv[0],max_help_position=70,width=200)
    parser = argparse.ArgumentParser(description='Custom OncodriveClust',formatter_class=lambda prog: help, )

    # Germline parameters
    parser.add_argument('--vcf', type=str, help='VCF file',required=True)
    parser.add_argument('--outdir', type=str, help='Output folder',required=True)
    parser.add_argument('--gver', type=str, help='Human genome release (hg19, hg20) [hg19]',required=False, default="hg19")
    parser.add_argument('--annovar', type=str, help='Annovar path',required=False, default=os.getenv('ANNOVAR_PATH',""))

    parser.add_argument('--gt', type=int, help='Minimum number of mutations per gene [3]',required=False, default=3)


    # PARSE
    args = parser.parse_args()

    if args.gver not in ["hg19","hg20"]:
        print "ERROR: Incorrect human genome release must be hg19 or hg20"
        exit()

    if args.annovar=="":
        print "ERROR: Annovar path not found (it can be specified by ANNOVAR_PATH environment variable)"
        exit()

    # run
    tool_path = os.path.abspath( os.path.dirname(sys.argv[0]))
    ofm = OncodriveClust(args.vcf,args.gt,args.gver,args.annovar,args.outdir,tool_path + "/data")
    ofm.run()


class OncodriveClust:

    def __init__(self,vcf,gt,gver,annovar,outdir,data_path):
        print "VCF: " + vcf
        print "gt: " + str(gt)
        print "gver: " + gver
        print "annovar: " + annovar
        print "outdir: " + outdir
        self.vcf = vcf
        self.gt = gt
        self.gver = gver
        if self.gver == "hg20": self.gver = "hg38"
        self.annovar = annovar
        self.outdir = outdir
        self.data_path = data_path


    def run(self):

        if os.path.exists(self.outdir):
            print "WARNING: Output directory already exists"
        else:
            os.mkdir(self.outdir)

        self.vcf_prefix = os.path.basename(self.vcf).replace(".vcf","").replace(".gz","")

        print "Annotating effects........"
        os.system(self.annovar +"/table_annovar.pl " + self.vcf + " " + self.annovar + "/humandb/ -buildver " + self.gver + " -out " + self.outdir + "/" + self.vcf_prefix + " -remove -protocol refGene,ensGene,ljb26_all -operation g,g,f -nastring . -vcfinput > " + self.outdir + "/annovar.o 2> " + self.outdir + "/annovar.e")
        os.system("cat " + self.outdir + "/" + self.vcf_prefix + "." + self.gver + "_multianno.vcf | sed 's/Gene.ensGene/Gene_ensGene/g' | sed 's/\.refGene/_refGene/g' | bcftools query -f \"%INFO/Gene_refGene\t%INFO/AAChange_refGene\t%INFO/Func_refGene\t%ExonicFunc_refGene[\t%SAMPLE=%GT]\n\" - | awk -F'\t' '{OFS=\"\t\"; split($2,pos,\":\"); ppos=pos[5]; if($3==\"exonic\") {for(i=5; i<=NF; i++) { split($i,a,\"=\"); if(a[2]==\"0/1\" || a[2]==\"1/1\") print $1,$2,$3,$4,a[1],substr(ppos,4,length(ppos)-4)}}}' > " + self.outdir + "/" + self.vcf_prefix + "_coding.txt ")
        os.system("awk -F'\t' '$4==\"synonymous_SNV\" || $4==\"unknown\"' " + self.outdir + "/" + self.vcf_prefix + "_coding.txt > " + self.outdir + "/" + self.vcf_prefix + "_coding_syn.txt")
        os.system("awk -F'\t' '$4!=\"synonymous_SNV\" && $4!=\"unknown\"' " + self.outdir + "/" + self.vcf_prefix + "_coding.txt > " + self.outdir + "/" + self.vcf_prefix + "_coding_nonsyn.txt")

        print " Running OncoDrive-Clust..."
        print("oncodriveclust -m " + str(self.gt) + " --cgc " + self.data_path + "/CGC_phenotype.tsv " + " -o " + self.outdir + "/" + self.vcf_prefix + "_results.txt " + self.outdir + "/" + self.vcf_prefix + "_coding_nonsyn.txt " + self.outdir + "/" + self.vcf_prefix + "_coding_syn.txt " + self.data_path + "/gene_transcripts.tsv > " + self.outdir + "/oncodriveclust.o 2> " + self.outdir + "/oncodriveclust.e")
        os.system("oncodriveclust --coord --dom " + self.data_path + "/pfam_domains.txt -m " + str(self.gt) + " --cgc " + self.data_path + "/CGC_phenotype.tsv " + " -o " + self.outdir + "/" + self.vcf_prefix + "_results.txt " + self.outdir + "/" + self.vcf_prefix + "_coding_nonsyn.txt " + self.outdir + "/" + self.vcf_prefix + "_coding_syn.txt " + self.data_path + "/gene_transcripts.tsv > " + self.outdir + "/oncodriveclust.o 2> " + self.outdir + "/oncodriveclust.e")

        self.formatResults()

        # RESULTS.XML
        rxml = ResultXML("oncodriveclust")

        rxml.addMetaDataItem("date","Date",time.strftime("%c"))

        rxml.addInputItem("vcf","VCF file",self.vcf)
        rxml.addInputItem("gt","Minimum number of mutations per gene",self.gt)
        rxml.addInputItem("gver","Human genome release",self.gver)
        rxml.addInputItem("outdir","Output directory",self.outdir)

        rxml.addOutputItem("vcf","VCF file","MESSAGE",os.path.basename(self.vcf),"INPUT_PARAM","Input parameters")
        rxml.addOutputItem("gt","Minimum number of mutations per gene","MESSAGE",self.gt,"INPUT_PARAM","Input parameters")
        rxml.addOutputItem("gver","Human genome release","MESSAGE",self.gver,"INPUT_PARAM","Input parameters")

        if self.nsig>0:
            rxml.addOutputItem("sig_count","Significant genes","MESSAGE","Found " + str(self.nsig) + " significant genes","","Output results")
        else:
            rxml.addOutputItem("sig_count","Significant genes","MESSAGE","No significant genes found","","Output results")
        rxml.addOutputItem("output_genes","Gene results","FILE",self.vcf_prefix + "_results.txt","TABLE,ONCODRIVECLUST-GENES","Output results")

        rxml.addOutputItem("fatiscan_redirection","Redirect ranked genes to Gene Set Enrichment","TEXT","","REDIRECTION(fatiscan.redirection:Send ranked genes to Gene Set Enrichment...)","Continue processing")
        rxml.addOutputItem("network_miner_redirection","Redirect ranked genes to Gene Set Network Enrichment","TEXT","","REDIRECTION(network_miner.redirection:Send ranked genes to Gene Set Network Enrichment...)","Continue processing")
        if self.nsig>0:
            rxml.addOutputItem("fatigo_redirection","Redirect significant genes to Single Enrichment","TEXT","","REDIRECTION(fatigo.redirection:Send to Single Enrichment...)","Continue processing")
            rxml.addOutputItem("snow_redirection","Redirect significant genes to Network Enrichment","TEXT","","REDIRECTION(snow.redirection:Send to Network Enrichment...)","Continue processing")
            rxml.addOutputItem("protein_viewer_significant","Significant genes network","FILE","significant_genes.txt","PROTEIN_VIEWER,HSA","Output results")

        rxml.save(self.outdir + "/result.xml")


    def formatResults(self):

        # reformat results
        fin = open(self.outdir + "/" + self.vcf_prefix + "_results.txt","r")
        rank = open(self.outdir + "/gene_rank.txt","w")
        sig_genes = open(self.outdir + "/significant_genes.txt","w")
        self.nsig = 0

        for line in fin:
            if line.find("#")!=0:
                if line.find("GENE")!=0:
                    fields = line.strip().split("\t")
                    if len(fields)>11 and fields[11] != "NA":
                        rank.write(fields[0] + "\t" + fields[9] + "\n")
                        if float(fields[11])<0.05:
                            sig_genes.write(fields[0]+"\n")
                            self.nsig = self.nsig + 1

        fin.close()
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



if __name__ == "__main__":
  main()

