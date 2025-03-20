#!/usr/bin/env python3

import individual_qc
import json
import logging
import os
import pyBigWig
import subprocess
from quality_control import *
from utils import *

LOGGER = logging.getLogger('variant_qc_logger')

ANALYSIS_NAME = "coverage based inferred sex analysis"
ANALYSIS_PATH = "coverage_based_inferred_sex"

# Get the directory of the script
script_directory = os.path.dirname(os.path.abspath(__file__))

class CoverageBasedInferredSexAnalysis:
	def __init__(self, executor):
		"""
		:param executor:
		"""

		self.name = ANALYSIS_NAME
		self.output_dir = create_output_dir(path_elements=[executor["output_parent_dir"], ANALYSIS_PATH])
		self.executor = executor
		self.inferred_sex = individual_qc.InferredSex()

	def setup(self):
		if self.executor == None:
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

	# Function to get the chromosome prefix in the BIGWIG file
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

	# Function to get the karyotypic sex for a given ratio_chrX and ratio_chrY
	def get_karyotypic_sex(self, ratio_chrX, ratio_chrY, thresholds):
		for key in thresholds:
			# Extract the label and the type of threshold (xmin, xmax, ymin, ymax)
			if key.endswith(".xmin"):
				# Remove ".xmin" to get the karyotypic sex
				karyotypic_sex = key[:-5]
				# Check if ratio_chrX and ratio_chrY falls within the range for this sex
				if (
					thresholds[f"{karyotypic_sex}.xmin"] <= ratio_chrX <= thresholds[f"{karyotypic_sex}.xmax"] and
					thresholds[f"{karyotypic_sex}.ymin"] <= ratio_chrY <= thresholds[f"{karyotypic_sex}.ymax"]
				):
					return karyotypic_sex.upper()

		# If no karyotypic sex matches, return UNKNOWN
		return "UNKNOWN"

	# Plot the karyotypic sex
	def plot_karyotypic_sex(self, ratio_chrX, ratio_chrY, thresholds_path, image_path):
		script_path = os.path.join(script_directory, "plot_karyotypic_sex.R")
		cmd = "Rscript " + script_path + " " + str(ratio_chrX) + " " + str(ratio_chrY) + " " + thresholds_path + " " + image_path
		LOGGER.info(f"Command: {cmd}")
		result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
		# Get the output
		if result.returncode == 0:
			LOGGER.info(f"Karyotypic sex image at {image_path}")
		else:
			LOGGER.info(f"Error: {result.stderr}")

    # Function to infer sex from the coverage (i.e., using a BIGWIG file)
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

		# Close the BigWig file
		bw.close()

		# Print the results
		LOGGER.info(f"Chromosome X coverage: {x_cov}")
		LOGGER.info(f"Chromosome Y coverage: {y_cov}")
		LOGGER.info(f"Average coverage of somatic chromosomes: {somatic_cov}")

		# Calculate ratio X-chrom / autosomes
		ratio_chrX = float(x_cov / somatic_cov)
		# Calculate ratio Y-chrom / autosomes
		ratio_chrY = float(y_cov / somatic_cov)

		LOGGER.info(f"Chromosome X ratio: {ratio_chrX}")
		LOGGER.info(f"Chromosome Y ratio: {ratio_chrY}")

		# Load the karyotypic sex thresholds from the JSON file
		thresholds_path = os.path.join(self.executor["resource_dir"], INFERRED_SEX_THRESHOLDS_FILE)
		with open(thresholds_path, "r") as file:
			thresholds = json.load(file)

		# Compute karyotypic sex
		karyotypic_sex = self.get_karyotypic_sex(ratio_chrX, ratio_chrY, thresholds)
		LOGGER.info(f"Karyotypic sex inferred: {karyotypic_sex}")

		# Method, sample ID
		self.inferred_sex.method = "Coverage based"
		self.inferred_sex.sampleId = self.executor["sample_ids"][0]

		# Software
		# self.inferred_sex.software =

		# Karyotypic sex
		self.inferred_sex.inferredKaryotypicSex = karyotypic_sex

		# Values
		self.inferred_sex.values["chrX_ratio"] = ratio_chrX
		self.inferred_sex.values["chrY_ratio"] = ratio_chrY
		self.inferred_sex.values["chrX_coverage"] = x_cov
		self.inferred_sex.values["chrY_coverage"] = y_cov
		self.inferred_sex.values["somatic_coverage"] = somatic_cov

		# Images
		image_fpath = os.path.join(self.output_dir, "coverage_based_inferred_sex.png")
		self.plot_karyotypic_sex(ratio_chrX, ratio_chrY, thresholds_path, image_fpath)
		base64_string = get_base64_image(image_fpath)
		self.inferred_sex.images = [Image(name=f"Inferred Sex (coverage based)", base64=base64_string)]

		# Attributes
		# self.inferred_sex.attributes =

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

			LOGGER.info('Complete successfully %s', ANALYSIS_NAME)
		except Exception as e:
			LOGGER.error("Error during %s: '%s'", ANALYSIS_NAME, format(e))
			raise





