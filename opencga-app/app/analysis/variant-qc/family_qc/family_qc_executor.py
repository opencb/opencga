#!/usr/bin/env python3

import json
import logging
import os

from common.relatedness_analysis import RelatednessAnalysis
from family_qc.family_quality_control import FamilyQualityControl
from utils import get_samples_info_from_individual, get_sample_id_from_individual_id, get_sample_info_from_individual_id

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

    def get_samples_info(self, family):
        samples_info = {}

        # Create samples info from members/individuals
        for member in family["members"]:
            samples_info.update(get_samples_info_from_individual(member, self.sample_ids))

        # Set roles
        if family["roles"]:
            for key, value in family["roles"].items():
                if samples_info[key]:
                    samples_info[key].roles.update(value)

        # Set father and mother sample IDs
        for member in family["members"]:
            if member["father"] and "id" in member["father"]:
                father_sample_id = get_sample_id_from_individual_id(member["father"]["id"], samples_info)
                sample_info = get_sample_info_from_individual_id(member["id"], samples_info)
                sample_info.fatherSampleId = father_sample_id
            if member["mother"] and "id" in member["mother"]:
                mother_sample_id = get_sample_id_from_individual_id(member["mother"]["id"], samples_info)
                sample_info = get_sample_info_from_individual_id(member["id"], samples_info)
                sample_info.motherSampleId = mother_sample_id

        # Set family ID
        for sample_info in samples_info.values():
            sample_info.familyIds = [self.id_]

        return samples_info

    def run(self):
        # Checking data
        # self.checking_data()  # TODO check input data (config parameters)

        # Reading info JSON file
        LOGGER.info(f"Getting family info from JSON file '{self.info_file}'")
        info_fhand = open(self.info_file, 'r')
        info_json = json.load(info_fhand)
        info_fhand.close()

        samples_info = self.get_samples_info(info_json)
        LOGGER.info(f"samples_info = {samples_info}")

        # Running individual QC steps
        # Get individual QC executor information
        executor_info = {
            "vcf_file": self.vcf_file,
            "info_file": self.info_file,
            "bam_file": self.bam_file,
            "config": self.config,
            "resource_dir": self.resource_dir,
            "output_parent_dir": self.output_parent_dir,
            "samples_info": samples_info,
            "id_": self.id_
        }

        LOGGER.info(f"Family QC executor info: {executor_info}")

        qc = FamilyQualityControl()

        # Run relatedness analysis
        relatedness_analysis = RelatednessAnalysis(executor_info)
        relatedness_analysis.run()
        qc.relatedness.append(relatedness_analysis.relatedness)

        # Write family quality control
        results_fpath = os.path.join(self.output_parent_dir, "family_quality_control.json")
        LOGGER.info('Generating JSON file with results. File path: "{}"'.format(results_fpath))
        with open(results_fpath, 'w') as file:
            json.dump(qc.model_dump(), file, indent=2)
            LOGGER.info('Finished writing JSON file with results: "{}"'.format(results_fpath))
