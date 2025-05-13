#!/usr/bin/env python3

import sys
import os
import logging
import json

from utils import create_output_dir, execute_bash_command
from sample_qc.mutational_catalogue import MutationalCatalogueAnalysis

LOGGER = logging.getLogger('variant_qc_logger')


class SampleQCExecutor:
    def __init__(self, vcf_file, info_file, bam_file, config, resource_dir, output_parent_dir, sample_ids, id_):
        """Create output dir

        :param str vcf_file: VCF input file path
        :param str info_file: Info JSON input file path
        :param str or None bam_file: BAM input file path
        :param str config: Configuration file path
        :param str resource_dir: Output directory path for resources
        :param str output_parent_dir: Output directory path for the id_ (e.g. /path/to/outdir/id1)
        :param list sample_ids: Sample IDs from the VCF file
        :param str id_: ID from the info JSON file
        """
        self.vcf_file = vcf_file
        self.info_file = info_file
        self.bam_file = bam_file
        self.config = config
        self.resource_dir = resource_dir
        self.output_parent_dir = output_parent_dir
        self.sample_ids = sample_ids
        self.id_ = id_

        # Loading configuration
        config_fhand = open(self.config, 'r')
        self.config_json = json.load(config_fhand)
        config_fhand.close()


    def run(self):

        # TODO check if sample is somatic

        # Genome plot
        if 'skip' not in self.config_json or 'genomePlot' not in self.config_json['skip']:
            self.create_genome_plot()

        # Mutational signatures
        if 'skip' not in self.config_json or 'mutationalCatalog' not in self.config_json['skip']:
            self.create_mutational_catalogue()


        # Return results
        # ...  # TODO return results
        pass

    def create_mutational_catalogue(self):
        # Create output dir for this analysis
        output_dir = create_output_dir([self.output_parent_dir, 'mutational_catalogue'])

        # Execute analysis
        mca = MutationalCatalogueAnalysis(self.vcf_file, self.resource_dir, self.config_json, output_dir, self.id_)
        mca.run()


    def get_genome_plot_vcfs(self, output_dir, gp_config_json):

        # Creating VCF files for each variant type
        snvs_fpath = os.path.join(output_dir, 'snvs.tsv')
        indels_fpath = os.path.join(output_dir, 'indels.tsv')
        cnvs_fpath = os.path.join(output_dir, 'cnvs.tsv')
        rearrs_fpath = os.path.join(output_dir, 'rearrs.tsv')

        # TODO Filtering VCF
        vcf_fhand = open(self.vcf_file, 'r')
        # /opencga-analysis/src/main/java/org/opencb/opencga/analysis/variant/genomePlot/GenomePlotLocalAnalysisExecutor.java
        vcf_fhand.close()

        return snvs_fpath, indels_fpath, cnvs_fpath, rearrs_fpath

    def create_genome_plot(self):
        # Creating output dir for this step
        output_dir = create_output_dir([self.output_parent_dir, 'genome-plot'])

        # Getting genome plot config file
        gp_config_fpath = self.config_json['genomePlot']['configFile']
        gp_config_fhand = open(gp_config_fpath, 'r')
        gp_config_json = json.load(gp_config_fhand)
        gp_config_fhand.close()

        # Getting variants
        snvs_fpath, indels_fpath, cnvs_fpath, rearrs_fpath = self.get_genome_plot_vcfs(output_dir, gp_config_json)

        # Creating CMD
        # /analysis/R/genome-plot/circos.R
        # https://github.com/opencb/opencga/blob/TASK-6766/opencga-analysis/src/main/R/genome-plot/circos.R
        # TODO input/output paths
        cmd = (
            ' R CMD Rscript --vanilla /data/input/circos.R'
            ' --genome_version hg38'
            ' --out_path /data/output'
            ' --plot_title {plot_title}'
            ' --out_format {out_format}'
            ' /data/output/{snvs_fname}'
            ' /data/output/{indels_fname}'
            ' /data/output/{cnvs_fname}'
            ' /data/output/{rearrs_fname}'
            ' {sample_id}'
        ).format(
            plot_title=gp_config_json['title'],
            out_format='png',
            snvs_fname=os.path.basename(snvs_fpath),
            indels_fname=os.path.basename(indels_fpath),
            cnvs_fname=os.path.basename(cnvs_fpath),
            rearrs_fname=os.path.basename(rearrs_fpath),
            sample_id=self.id_
        )

        return_code, stdout, stderr = execute_bash_command(cmd)
