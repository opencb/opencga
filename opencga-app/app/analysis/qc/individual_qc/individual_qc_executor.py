#!/usr/bin/env python3
import os
import logging
import json

import individual_qc

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

	def run(self):
		# Checking data
		# self.checking_data()  # TODO check input data (config parameters)

		# Reading info JSON file
		LOGGER.info(f"Getting individual info from JSON file '{self.info_file}'")
		info_fhand = open(self.info_file, 'r')
		info_json = json.load(info_fhand)
		info_fhand.close()

		# Get sample ID
		sample_id = None
		if info_json.get("samples") and "id" in info_json["samples"][0]:
			sample_id = info_json["samples"][0]["id"]
			if not sample_id in self.sample_ids:
				sample_id = None
		LOGGER.info(f"Got sample ID '{sample_id}' for individual ID '{self.id_}'")

		# Get father and mother IDs
		father_id = None
		if info_json.get("father") and "id" in info_json["father"]:
			father_id = info_json["father"]["id"]
			if not father_id in self.sample_ids:
				father_id = None
		mother_id = None
		if info_json.get("mother") and "id" in info_json["mother"]:
			mother_id = info_json["mother"]["id"]
			if not mother_id in self.sample_ids:
				mother_id = None
		LOGGER.info(f"Got father ID '{father_id}' and mother ID '{mother_id}' for individual ID '{self.id_}'")

		# Get sex
		sex = 0
		if info_json.get("sex") and "id" in info_json["sex"]:
			if info_json["sex"]["id"] == "MALE":
				sex = 1
			elif info_json["sex"]["id"] == "FEMALE":
				sex = 2
		LOGGER.info(f"Got sex '{sex}' for individual ID '{self.id_}'")

		# Running individual QC steps
		# Get individual QC executor information
		executor_info = {
			"vcf_file": self.vcf_file,
			"info_file": self.info_file,
			"bam_file": self.bam_file,
			"config": self.config,
			"resource_dir": self.resource_dir,
			"output_parent_dir": self.output_parent_dir,
			"sample_ids": self.sample_ids,
			"sample_id": sample_id,
			"sex": sex,
			"father_id": father_id,
			"mother_id": mother_id,
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

		# Run relatedness analysis if parents are provided

		# Run mendelian errors analysis if parents are provided
		mendelian_errors_analysis = individual_qc.MendelianErrorsAnalysis(executor_info)
		if sample_id != None and father_id != None and mother_id != None:
			mendelian_errors_analysis = individual_qc.MendelianErrorsAnalysis(executor_info)
			mendelian_errors_analysis.run()
			qc.mendelianErrors.append(mendelian_errors_analysis.mendelian_errors)
		else:
			LOGGER.warning(f"Sample, father and/or mother are not provided. Skipping {mendelian_errors_analysis.name}")

		# Write individual quality control
		results_fpath = os.path.join(self.output_parent_dir, "individual_quality_control.json")
		LOGGER.info('Generating JSON file with results. File path: "{}"'.format(results_fpath))
		with open(results_fpath, 'w') as file:
			json.dump(qc.model_dump(), file, indent=2)
			LOGGER.info('Finished writing JSON file with results: "{}"'.format(results_fpath))



