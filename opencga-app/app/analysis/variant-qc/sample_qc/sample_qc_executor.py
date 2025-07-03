#!/usr/bin/env python3

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
        # Run the next analyses if sample is somatic
        if self.sample_is_somatic():
            # Mutational signatures
            if 'skip' not in self.config_json or 'mutationalCatalog' not in self.config_json['skip']:
                self.create_mutational_catalogue()

    def sample_is_somatic(self):
        info_fhand = open(self.info_file, 'r')
        info_json = json.load(info_fhand)
        info_fhand.close()
        if info_json['somatic']:
            return True
        else:
            return False


    def create_mutational_catalogue(self):
        # Create output dir for this analysis
        output_dir = create_output_dir([self.output_parent_dir, 'mutational_catalogue'])

        # Execute analysis
        mca = MutationalCatalogueAnalysis(self.vcf_file, self.resource_dir, self.config_json, output_dir, self.id_)
        mca.run()
