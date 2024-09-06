#!/usr/bin/env python3

import sys
import os
import logging


from utils import create_output_dir, execute_bash_command

LOGGER = logging.getLogger('variant_qc_logger')

class SampleQCExecutor:
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
        # check_data()
        # relatedness()
        # inferred_sex()
        # if info_file.somatic == True && config.mutational_signature.skip == False:
        #     mutational_signature()
        # mendelian_errors()
        # return ;

        self.bcftools_stats(vcf_file=self.vcf_file)

        # missingness()
        # heterozygosity ()
        # roh()
        # upd()

        # Return results
        # ...  # TODO return results
        pass


    def bcftools_stats(self, vcf_file):
        """
        Calculates VCF stats using BCFTools
        :param str vcf_file: VCF file to get stats from
        :return:
        """
        # Creating output dir for bcftools
        output_dir = create_output_dir([self.output_parent_dir, 'bcftools'])

        # Running bcftools
        cmd_bcftools = 'bcftools stats -v ' + vcf_file + ' > ' + os.path.join(output_dir, 'bcftools_stats.txt')
        execute_bash_command(cmd=cmd_bcftools)
        LOGGER.info("BCFTools stats calculated successfully for {file}".format(file=vcf_file))

        # Plot stats using plot-vcfstats
        cmd_vcfstats = 'plot-vcfstats -p ' + output_dir + '/bcftools_stats_plots ' + output_dir + '/bcftools_stats.txt'
        execute_bash_command(cmd=cmd_vcfstats)
        LOGGER.info("Plot stats using plot-vcfstats executed successfully for {file}".format(file=vcf_file))

        # TODO: Future implementation for vcf stats plots
        #plot_bcftools_stats(file=output_dir + '/bcftools_stats.txt', prefix="stats", output=output_dir)


if __name__ == '__main__':
    vcf_file = "/home/mbleda/Downloads/KFSHRC/CGMQ2022-02850.vcf.gz"
    info_file = ""
    bam_file = ""
    config = ""
    output_parent_dir = "/tmp/qc_tests/"
    job_id = ""
    sample_ids = []
    id_ = ""
    se = SampleQCExecutor(vcf_file=vcf_file, info_file=info_file, bam_file=bam_file, config=config,
                          output_parent_dir=output_parent_dir, job_id=job_id, sample_ids=sample_ids, id_=id_)
    sys.exit(se.run())
