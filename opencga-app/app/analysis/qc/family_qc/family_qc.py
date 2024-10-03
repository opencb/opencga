#!/usr/bin/env python3
import os
import logging
import json

from family_qc.relatedness import RelatednessAnalysis

LOGGER = logging.getLogger('variant_qc_logger')


class FamilyQCExecutor:
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
        # Checking data
        # self.checking_data()  # TODO check input data (config parameters)

        # Running family QC steps
        # Get family QC executor information
        family_qc_executor_info = {
            "vcf_file": self.vcf_file,
            "info_file": self.info_file,
            "bam_file": None,
            "config": self.config,
            "resource_dir": self.resource_dir,
            "output_parent_dir": self.output_parent_dir,
            "sample_ids": self.sample_ids,
            "id_": self.id_
            }
        # Run relatedness analysis
        relatedness_analysis = RelatednessAnalysis(family_qc_executor_info)
        relatedness_analysis.run()





