#!/usr/bin/env python3

import subprocess
import sys
#import bokeh
import os
import logging

from utils import create_output_dir

LOGGER = logging.getLogger('variant_qc_logger')

class IndividualQCExecutor:
    def __init__(self, vcf_file, info_file, bam_file, config, output_parent_dir, job_id, sample_ids, id_):
        """Create output dir

        :param str vcf_file: VCF input file path
        :param str info_file: Info JSON input file path
        :param str or None bam_file: BAM input file path
        :param str config: Configuration file path
        :param str output_parent_dir: Output directory path for the id_ (e.g. /path/to/outdir/id1)
        :param list sample_ids: Sample IDs from the VCF file
        :param str job_id: ID from the executed job
        :param str id_: ID from the info JSON file
        """
        self.vcf_file = vcf_file
        self.info_file = info_file
        self.bam_file = bam_file
        self.config = config
        self.output_parent_dir = output_parent_dir
        self.job_id = job_id
        self.sample_ids = sample_ids
        self.id_ = id_

    def run(self):
        # Checking data
        # self.checking_data()  # TODO check input data

        # Running sample QC steps
        # self.step1()  # TODO run all encessary steps for this QC (e.g. relatedness)

        # Return results
        # ...  # TODO return results
        pass

    def step1(self):
        # Create output dir for this step
        output_dir = create_output_dir([self.output_parent_dir, 'step1'])

        # Run step1
        # ...  # TODO execute this step commands
        pass

vcf_file = "/home/mbleda/Downloads/KFSHRC/CGMQ2022-02850.vcf.gz"
output_dir = "/tmp/qc_tests/"
sample_ids = ""
info_file = ""
config = ""


def run(vcf_file, sample_ids, info_file, config, output_dir):
    inferred_sex(vcf_file=vcf_file, output_dir=output_dir)
    # missingness()
    # heterozygosity ()
    # roh()
    # upd()

def exec_bash_command(cmd_line):
    """
    Run a bash command (e.g. bcftools), and return output
    """
    po = subprocess.Popen(cmd_line,
                          shell=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)

    stdout, stderr = po.communicate()
    po.wait()
    return_code = po.returncode

    if return_code != 0:
        raise Exception(
            "Command line {} got return code {}.\nSTDOUT: {}\nSTDERR: {}".format(cmd_line, return_code, stdout,
                                                                                 stderr))

    return po.returncode, stdout


def bcftools_stats(vcf_file, output_dir):
    """
    Calculates VCF stats using BCFTools
    :param vcf_file: VCF file to get stats from
    :param output_dir: Output directory
    :return:
    """
    bcftools_stats_output = exec_bash_command(cmd_line='bcftools stats -v ' + vcf_file + ' > ' + output_dir + '/bcftools_stats.txt')
    if bcftools_stats_output[0] == 0:
        print("BCFTools stats calculated successfully for {file}".format(file=vcf_file))
        #plot_bcftools_stats(file=output_dir + '/bcftools_stats.txt', prefix="stats", output=output_dir)

def inferred_sex(vcf_file, output_dir):
    """
    Calculates inferred sex and checks if matches against the expected
    :param vcf_file: VCF file containing variants for chrX and chrY. This VCF should have
    been previously filtered to contain variants
    :param output_dir: Output directory
    :return:
    """
    binary = "/home/mbleda/soft/plink/plink_linux_x86_64_20231018/plink"
    plink_sex_output = exec_bash_command(cmd_line=binary + 'bcftools stats -v ' + vcf_file + ' > ' + output_dir + '/bcftools_stats.txt')
    # if bcftools_stats_output[0] == 0:
    #     print("BCFTools stats calculated successfully for {file}".format(file=vcf_file))
    #     plot_bcftools_stats(file=output_dir + '/bcftools_stats.txt', prefix="stats", output=output_dir)

if __name__ == '__main__':
    sys.exit(run(vcf_file=vcf_file, sample_ids=sample_ids, info_file=info_file, config=config, output_dir=output_dir))
