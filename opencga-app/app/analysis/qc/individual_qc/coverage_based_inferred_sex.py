#!/usr/bin/env python3

import os
import logging
import gzip
import json
import pyBigWig

from utils import create_output_dir, execute_bash_command, generate_results_json


LOGGER = logging.getLogger('variant_qc_logger')
ANALYSIS_NAME = "coverage based inferred sex analysis"

class CoverageBasedInferredSexAnalysis:
	def __init__(self, executor):
		"""
		:param executor:
		"""

		self.output_coverage_based_inferred_sex_dir = None
		self.executor = executor
		LOGGER.info("executor = %s", executor)

	def setup(self):
		if self.executor != None:
			LOGGER.info("bam file = %s", self.executor["bam_file"])
#             self.set_variant_based_inferred_sex_files()
#             self.set_variant_based_inferred_sex_dir()
		else:
			msg = "No instance of IndividualQCExecutor was found. Therefore coverage based inferred sex analysis cannot be executed."
			LOGGER.error(msg)
			raise TypeError(msg)

	# Function to calculate coverage for a chromosome
	def calculate_coverage(self, chromosome, bw):
		if chromosome not in bw.chroms():
			print(f"Chromosome {chromosome} not found in the BigWig file.")
			return 0.0

		# Get the chromosome length
		chrom_length = bw.chroms(chromosome)

		# Calculate the mean coverage
		coverage = bw.stats(chromosome, 0, chrom_length, type="mean")
		return coverage[0] if coverage else 0.0

	def detect_chromosome_prefix(self, bw):
		# Get the chromosome names
		chromosome_names = list(bw.chroms().keys())

		if all(chrom.startswith("chr") for chrom in chromosome_names):
			# All chromosomes start with "chr"
			return "chr"
		elif all(chrom.startswith("Chr") for chrom in chromosome_names):
			# All chromosomes start with "Chr"
			return "Chr"
		elif all(not (chrom.startswith("chr") or chrom.startswith("Chr")) for chrom in chromosome_names):
			# No chromosomes have a prefix
			return ""
		else:
			msg = "Mixed or unknown pattern in chromosome names in BigWig file."
			LOGGER.error(msg)
			raise TypeError(msg)

	def calculate_inferred_sex(self):
		"""
		Calculates inferred sex based on coverage if BAM information is available and checks if matches against the expected
		:return:
		"""

		# Open the BigWig file
		bw = pyBigWig.open(self.executor["bam_file"])

		# Detect the prefix
		prefix = self.detect_chromosome_prefix(bw)
		LOGGER.info(f"Prefix detected '{prefix}' in chromosome names in the BigWig file")

		# Define chromosome groups
		x_chr = prefix + "X"
		y_chr = prefix + "Y"
		somatic_chr = [f"{prefix}{i}" for i in range(1, 23)]  # {prefix}1 to {prefix}22

		# Calculate coverage for X, Y, and somatic chromosomes
		x_cov = self.calculate_coverage(x_chr, bw)
		y_cov = self.calculate_coverage(y_chr, bw)
		somatic_cov = sum(self.calculate_coverage(chr, bw) for chr in somatic_chr) / len(somatic_chr)

		# Print the results
		print(f"Coverage for X chromosome: {x_cov}")
		print(f"Coverage for Y chromosome: {y_cov}")
		print(f"Average coverage for somatic chromosomes: {somatic_cov}")

		# Calculate ratio X-chrom / autosomes
		ratio_chrX = float(x_cov / somatic_cov)
		# Calculate ratio Y-chrom / autosomes
		ratio_chrY = float(y_cov / somatic_cov)

		print(f"Ratio for X chromosome: {ratio_chrX}")
		print(f"Ratio for Y chromosome: {ratio_chrY}")

		# Close the BigWig file
		bw.close()

	def run(self):
		"""
		Execute the coverage based inferred sex analysis
		"""
		try:
			LOGGER.info('Starting %s', ANALYSIS_NAME)

			# Input files set up
			self.setup()

			# Performing IBD analysis from PLINK
			self.calculate_inferred_sex()

#             plink_genome_fpath = self.relatedness_plink(filtered_vcf_fpath)
#
#             # Getting and calculating relatedness scores: reported relationship, inferred relationship, validation
#             self.relatedness_scores(plink_genome_fpath)
#
#             # Generating file with results
#             generate_results_json(self.relatedness_results.model_dump(),self.output_relatedness_dir)
			LOGGER.info('Complete successfully %s', ANALYSIS_NAME)

		except Exception as e:
			LOGGER.error("Error during %s: '%s'", ANALYSIS_NAME, format(e))
			raise





