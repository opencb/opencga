#!/usr/bin/env python3
import os
import logging
import json
import sys

from utils import *
import individual_qc
import common

LOGGER = logging.getLogger('variant_qc_logger')

class IndividualQCExecutor:
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

	def is_individual_sample(self, individual, sample_id):
		if individual.get("samples"):
			for sample in individual.get("samples"):
				if sample["id"] == sample_id:
					return True
		return False

	def get_samples_info_from_individual(self, individual):
		samples_info = {}

		if individual.get("samples"):
			for sample in individual.get("samples"):
				sample_id = sample["id"]
				if  sample_id in self.sample_ids:
					info = SampleInfo(sampleId=sample_id,
										fatherSampleId="0",
										motherSampleId="0",
									  	individualId=individual.get("id"),
									  	familyIds=individual["familyIds"],
									  	roles={},
										sex=get_individual_sex(individual),
										phenotype=get_individual_phenotype(individual))

					samples_info[sample_id] = info

		return samples_info

	def get_sample_id_by_individual_id(self, samples_info, individual_id):
		for sample_id, sample_info in samples_info.items():
			if sample_info.individualId == individual_id:
				return sample_id
		return None

	def get_samples_info(self, individual):
		samples_info = {}

		# Get sample info from the parents and child
		samples_info.update(self.get_samples_info_from_individual(individual))
		father_id = None
		if individual["father"] and individual["father"]["id"]:
			father_id = individual["father"]["id"]
			samples_info.update(self.get_samples_info_from_individual(individual["father"]))
		mother_id = None
		if individual["mother"] and individual["mother"]["id"]:
			mother_id = individual["mother"]["id"]
			samples_info.update(self.get_samples_info_from_individual(individual["mother"]))

		# Set parents info
		if father_id != None and mother_id != None:
			father_sample_id = self.get_sample_id_by_individual_id(samples_info, father_id)
			mother_sample_id = self.get_sample_id_by_individual_id(samples_info, mother_id)
			if father_sample_id != None and mother_sample_id != None:
				for sample_info in samples_info.values():
					if sample_info.individualId == individual["id"]:
						if sample_info.sex == 2:
							role = "DAUGHTER"
						else:
							role = "SON"
						# Father and child
						sample_info.fatherSampleId = father_sample_id
						samples_info[father_sample_id].roles[sample_info.sampleId] = role
						samples_info[sample_info.sampleId].roles[father_sample_id] = "FATHER"
						# Mother and child
						sample_info.motherSampleId = mother_sample_id
						samples_info[mother_sample_id].roles[sample_info.sampleId] = role
						samples_info[sample_info.sampleId].roles[mother_sample_id] = "MOTHER"
				# Father and mother
				samples_info[father_sample_id].roles[mother_sample_id] = "SPOUSE"
				samples_info[mother_sample_id].roles[father_sample_id] = "SPOUSE"

		# Extract familyIds lists as sets, and find the intersection
		family_sets = [set(value.familyIds) for value in samples_info.values()]
		common_family_ids = list(set.intersection(*family_sets))
		if len(common_family_ids) == 0:
			raise Exception("Family ID is missing or does not match between samples")
		for sample_info in samples_info.values():
			sample_info.familyIds = [common_family_ids[0]]

		return samples_info

	def run(self):
		# Checking data
		# self.checking_data()  # TODO check input data (config parameters)

		# Reading info JSON file
		LOGGER.info(f"Getting individual info from JSON file '{self.info_file}'")
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

		LOGGER.info(f"Individual QC executor info: {executor_info}")

		qc = individual_qc.IndividualQualityControl()

		# Run coverage based inferred sex analysis if BIGWIG file is provided
		coverage_inferred_sex_analysis = individual_qc.CoverageBasedInferredSexAnalysis(executor_info)
		if self.bam_file != None:
			if os.path.isfile(self.bam_file):
				coverage_inferred_sex_analysis.run()
				qc.inferredSex.append(coverage_inferred_sex_analysis.inferred_sex)
			else:
				msg = "File '{}' does not exist".format(self.bam_file)
				LOGGER.error(msg)
				raise FileNotFoundError(msg)
		else:
			LOGGER.warning(f"BAM file is not provided. Skipping {coverage_inferred_sex_analysis.name}")

		# Run variant based inferred sex analysis
		variant_inferred_sex_analysis = individual_qc.VariantBasedInferredSexAnalysis(executor_info)
		variant_inferred_sex_analysis.run()
		qc.inferredSex.append(variant_inferred_sex_analysis.inferred_sex)

		relatedness_analysis = common.RelatednessAnalysis(executor_info)
		mendelian_errors_analysis = individual_qc.MendelianErrorsAnalysis(executor_info)
		if contains_trio(samples_info):
			# Run relatedness analysis if parents are provided
			relatedness_analysis.run()
			qc.relatedness.append(relatedness_analysis.relatedness)

			# Run mendelian errors analysis if parents are provided
			mendelian_errors_analysis.run()
			qc.mendelianErrors.append(mendelian_errors_analysis.mendelian_errors)
		else:
			LOGGER.warning(f"Sample, father and/or mother are not provided. Skipping {relatedness_analysis.name}"
						   f" and {mendelian_errors_analysis.name}")

		# Write individual quality control
		results_fpath = os.path.join(self.output_parent_dir, "individual_quality_control.json")
		LOGGER.info('Generating JSON file with results. File path: "{}"'.format(results_fpath))
		with open(results_fpath, 'w') as file:
			json.dump(qc.model_dump(), file, indent=2)
			LOGGER.info('Finished writing JSON file with results: "{}"'.format(results_fpath))



