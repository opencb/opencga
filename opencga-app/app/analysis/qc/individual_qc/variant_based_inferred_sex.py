#!/usr/bin/env python3

import os
import logging
import gzip
import json
import subprocess
import base64

from utils import RESOURCES_FILENAMES, create_output_dir, execute_bash_command, generate_results_json
from individual_qc.inferred_sex_result import InferredSexResult, Software, Image

LOGGER = logging.getLogger('variant_qc_logger')
ANALYSIS_NAME = "variant based inferred sex analysis"

# Get the directory of the script
script_directory = os.path.dirname(os.path.abspath(__file__))

class VariantBasedInferredSexAnalysis:
	def __init__(self, executor):
		"""
		:param executor:
		"""

		output_dir = create_output_dir(path_elements=[executor["output_parent_dir"], 'variant_based_inferred_sex'])
		self.output_dir = output_dir

		self.chrx_vars_fpath = None
		self.chrx_var_frq_fpath = None
		self.sexcheck_ref_values_fpath = None
		self.executor = executor
		self.inferred_sex_result = InferredSexResult()

	def setup(self):
		if self.executor != None:
			self.set_variant_based_inferred_sex_files()
		else:
			msg = "No instance of IndividualQCExecutor was found. Therefore variant based inferred sex analysis cannot be executed."
			LOGGER.error(msg)
			raise TypeError(msg)

	def set_variant_based_inferred_sex_files(self):
		LOGGER.info("Checking and setting up variant based inferred sex files.")
		resources_path = self.executor["resource_dir"]
		if os.path.exists(resources_path):
			check_sex_files = {
				"chrx_vars": os.path.join(resources_path, RESOURCES_FILENAMES["INFERRED_SEX_CHR_X_PRUNE_IN"]),
				"chrx_var_frq": os.path.join(resources_path, RESOURCES_FILENAMES["INFERRED_SEX_CHR_X_FRQ"]),
				"sexcheck_ref_values": os.path.join(resources_path, RESOURCES_FILENAMES["INFERRED_SEX_REFERENCE_VALUES"])
			}
			for key,file in check_sex_files.items():
				if os.path.isfile(file):
					if key == "chrx_vars":
						self.chrx_vars_fpath = file
					elif key == "chrx_var_frq":
						self.chrx_var_frq_fpath = file
					elif key == "sexcheck_ref_values":
						self.sexcheck_ref_values_fpath = file
					LOGGER.info("File '{}' set up successfully".format(file))
				else:
					msg = "File '{}' does notxist".format(file)
					LOGGER.error(msg)
					raise FileNotFoundError(msg)
		else:
			msg = "Directory '{}' does not exist".format(resources_path)
			LOGGER.error(msg)
			raise FileNotFoundError(msg)

	def filter_variants(self):
		"""
		Annotates input VCF with chromosome coordinate IDs and filters for pruned PASS chr X variants using BCFTools.
		:return:
		"""
		# Read in VCF:
		vcf_file = self.executor["vcf_file"] #gzip.open(self.executor["vcf_file"], "r")
		# Read in list of variants to filter for:
		good_vars = open(self.chrx_vars_fpath, "r")

		# Create output file:
		for sample_id in self.executor["sample_ids"]:
			individual_id = self.executor["id_"]
			filtered_vcf_name = individual_id + "_" + sample_id + "_filtered_variants.vcf.gz"
			filtered_vcf_path = os.path.join(self.output_dir, filtered_vcf_name)
			LOGGER.debug("Generating VCF filtered for good quality pruned chr X variants: '{}'".format(filtered_vcf_path))

			# Check if input VCF FILTER field contains PASS variants:
			LOGGER.info(f"Checking for PASS variants in the FILTER field of {vcf_file}")
			cmd = f"bcftools view -i 'FILTER=\"PASS\"' {vcf_file} | grep -v '^#\|CHROM' | wc -l"
			LOGGER.info(f"Command: {cmd}")
			result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
			# Get the output
			if result.returncode == 0:
				pass_vars = int(result.stdout.strip())
				LOGGER.info(f"Number of PASS variants: {pass_vars}")
			else:
				LOGGER.info(f"Error: {result.stderr}")

			# Annotate input VCF with chr coordinate IDs, filter for chr X pruned variants and, if annotated, for PASS variants:
			if pass_vars == 0:
# 				execute_bash_command(cmd=annotate_and_filter_no_pass_cmd)
				LOGGER.debug("Annotating and filtering '{}' for all pruned chr X variants".format(vcf_file))
				LOGGER.info("WARNING: no FILTER information available, input data will not be filtered for PASS variants, results may be unreliable")
				cmd = "bcftools annotate --set-id '%CHROM\:%POS\:%REF\:%FIRST_ALT' " + vcf_file + " -Oz | bcftools view --include ID==@" + self.chrx_vars_fpath + " -Oz -o " + filtered_vcf_path
				LOGGER.info(f"Command: {cmd}")
				result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
				if result.returncode != 0:
					LOGGER.info(f"Error annotating and filtering: {result.stderr}")
			else:
				LOGGER.debug("Annotating and filtering '{}' for chr X pruned PASS variants".format(vcf_file))
# 				execute_bash_command(cmd=annotate_and_filter_pass_cmd)
				cmd = "bcftools annotate --set-id '%CHROM\:%POS\:%REF\:%FIRST_ALT' " + vcf_file + " -Oz | bcftools view --include ID==@" + self.chrx_vars_fpath + " -Oz | bcftools view -i 'FILTER=\"PASS\"' -Oz -o " + filtered_vcf_path
				LOGGER.info(f"Command: {cmd}")
				result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
				if result.returncode != 0:
					LOGGER.info(f"Error annotating and filtering: {result.stderr}")

		# Return filtered VCF path:
		return [filtered_vcf_path, filtered_vcf_name]

	def variant_check(self, filtered_vcf_path, filtered_vcf_name):
		# Calculate number of variants remaining for Plink check-sex/impute-sex after all filtering
		cmd = "zgrep -v '^#\|CHROM' " + filtered_vcf_path + " | wc -l"
		LOGGER.info(f"Command: {cmd}")
		result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
		# Get the output
		if result.returncode == 0:
			filtered_vars = int(result.stdout.strip())
			LOGGER.info(f"Checking final number of variants in {filtered_vcf_name}")
			LOGGER.info(f"Number of variants: {filtered_vars}")
		else:
			LOGGER.info(f"Error: {result.stderr}")

		return filtered_vars

	def get_individual_sex(self):
		"""
		Retrieve individual sex for sample if it is available, and recode for Plink input.
		:return:
		"""
		individual_info = open(self.executor["info_file"])
		individual_info_json = json.load(individual_info)
		indsex = individual_info_json['sex']['id']
		karsex = individual_info_json['karyotypicSex']
		ind_metadata = {}
		for sample in self.executor["sample_ids"]:
			ind_metadata[sample] = {'individualId': individual_info_json['id'], 'individualSex': 0, 'sampleId': 'NA'}

			LOGGER.debug("Retrieve sex information and recode")
			for sam in individual_info_json['samples']:
				if sam['id'] in self.executor["sample_ids"]:
					ind_metadata[sam['id']]['sampleId'] = sample
					if indsex.upper() == 'MALE' or karsex == 'XY':
						ind_metadata[sam['id']]['individualSex'] = 1
					elif indsex.upper() == 'FEMALE' or karsex == 'XX':
						ind_metadata[sam['id']]['individualSex'] = 2
					else:
						LOGGER.info("Sex information for Individual '{}' (sample '{}') is not available, sex will be inferred".format(sam['id'],sample['id']))

		# Check individual information for each sample is present:
		for sample, individual_info in ind_metadata.items():
			if individual_info['individualSex'] == 0:
				LOGGER.warning("No individual information available for sample '{}'".format(individual_info['sampleId']))
			else:
				LOGGER.info("Individual information for sample '{}' found".format(individual_info['sampleId']))

		# Return sex information:
		return ind_metadata

	def generate_plink_fam_file_input(self):
		# Retrieve sex information:
		individual_id = self.executor["id_"]
		ind_metadata = self.get_individual_sex()

		# Create a directory for Plink if it does not already exist:
		plink_dir = create_output_dir(path_elements=[self.output_dir, 'plink_check-sex'])

		# Generate a text file to update the Plink fam file with sex information:
		sex_info_file_name = individual_id + '_sex_information.txt'
		sex_info_path = os.path.join(plink_dir, sex_info_file_name)
		sex_info_input = open(sex_info_path, 'w')
		LOGGER.debug("Generating text file to update the Plink input sex information: '{}'".format(sex_info_path))

		for sample in self.executor["sample_ids"]:
			# Individual sample information:
			individual_info = ind_metadata[sample]
			# Plink sex update file format: familyId individualId sex
			sex_info_input.write(('\t'.join([sample,sample,str(individual_info['individualSex'])])) + '\n')
			# Note, for the purpose of this function, individualId replaces familyId to prevent issues where an individual belongs to more than one family
			LOGGER.info("Test file generated to update the Plink fam file with sex information: '{}'".format(sex_info_path))

		# Return paths of text files generated:
		return sex_info_path

	def check_sex_plink(self, filtered_vcf_path, plink_path="plink1.9"):
		method = "Plink/check-sex"
		# Define Plink check-sex methodology:
		LOGGER.info("Method: {}".format(method))

		# Create a directory for Plink if it does not already exist:
		plink_dir = create_output_dir(path_elements=[self.output_dir, 'plink_check-sex'])
		sex_info_path = self.generate_plink_fam_file_input()

		# Convert input to Plink format:
		plink_path = str(plink_path)
		for sample_id in self.executor["sample_ids"]:
			file_prefix = self.executor["id_"] + "_" + sample_id + "_plink_check-sex_results"
			plink_output_prefix = os.path.join(plink_dir, file_prefix)
			plink_convert_args = ["--vcf", str(filtered_vcf_path),
									"--make-bed",
									"--keep-allele-order",
									"--update-sex", sex_info_path,
									"--split-x", "hg38", "no-fail",
									"--out", plink_output_prefix]
			cmd = [plink_path] + plink_convert_args
			LOGGER.info(f"Generating Plink check-sex input files")
# 			plink_input_files = execute_bash_command(plink_convert_cmd)
# 			LOGGER.info(f"Command: {' '.join(cmd)}")
# 			result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
			[returncode, stdout, stderr] = execute_bash_command(cmd)
# 			LOGGER.info(f"returncode = {returncode}")
# 			LOGGER.info(f"stdout = {stdout}")
# 			LOGGER.info(f"stderr = {stderr}")
			# Get the output
			if returncode == 0:
				plink_files_generated = os.listdir(plink_dir)
				LOGGER.info("Files available: '{}':\n{}".format(plink_dir, plink_files_generated))

				# Run Plink check-sex analysis:
				plink_checksex_args = ["--bfile", plink_output_prefix,
										"--read-freq", str(self.chrx_var_frq_fpath),
										"--check-sex",
										"--out", plink_output_prefix]
				cmd = [plink_path] + plink_checksex_args
				LOGGER.debug("Performing Plink check-sex")
# 				plink_checksex = execute_bash_command(plink_checksex_cmd)
# 				LOGGER.info(f"Command: {' '.join(cmd)}")
# 				result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
				[returncode, stdout, stderr] = execute_bash_command(cmd)
# 				LOGGER.info(f"returncode = {returncode}")
# 				LOGGER.info(f"stdout = {stdout}")
# 				LOGGER.info(f"stderr = {stderr}")
				# Get the output
				if returncode == 0:
					checksex_files_generated = os.listdir(plink_dir)
					LOGGER.info(f"Files available at '{plink_dir}': {checksex_files_generated}")
				else:
					LOGGER.error(f"Error {stderr} executing PLINK command: {cmd}")
					raise Exception("PLINK error")

				sexcheck_path = plink_output_prefix + '.sexcheck'
				if os.path.isfile(sexcheck_path) == False:
 					LOGGER.error(f"File '{sexcheck_path}' does not exist")
# 					LOGGER.error("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_checksex_path, plink_checksex[1], plink_checksex[2]))
# 					raise Exception("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_checksex_path, plink_checksex[1], plink_checksex[2]))
				else:
					# Return method used and path to the .sexcheck output file generated by Plink
					return [method, sexcheck_path]
			else:
				LOGGER.error(f"Error {stderr} executing PLINK command: {cmd}")
				raise Exception("PLINK error")

	def impute_sex_plink(self, filtered_vcf_path, plink_path="plink1.9"):
		method = "Plink/impute-sex"
		# Define Plink impute-sex methodology:
		LOGGER.info("Method: {}".format(method))

		# Create a directory for Plink if it does not already exist:
		plink_dir = create_output_dir(path_elements=[self.output_dir, 'plink_impute-sex'])

		# Convert input to Plink format:
		plink_path = str(plink_path)
		for sample_id in self.executor["sample_ids"]:
			file_prefix = self.executor["id_"] + "_" + sample_id + "_plink_impute_sex_results"
			plink_output_prefix = os.path.join(plink_dir, file_prefix)
			plink_convert_args = ["--vcf", str(filtered_vcf_path),
									"--make-bed",
									"--keep-allele-order",
									"--split-x", "hg38", "no-fail",
									"--out", plink_output_prefix]
			cmd = [plink_path] + plink_convert_args
			LOGGER.debug("Generating Plink impute-sex input files")
			[returncode, stdout, stderr] = execute_bash_command(cmd)
			if returncode == 0:
				plink_files_generated = os.listdir(plink_dir)
				LOGGER.info("Files available: '{}':\n{}".format(plink_dir, plink_files_generated))

				# Run Plink impute-sex analysis:
				plink_imputesex_args = ["--bfile", plink_output_prefix,
										"--read-freq", str(self.chrx_var_frq_fpath),
										"--impute-sex",
										"--make-bed",
										"--out", plink_output_prefix]
				cmd = [plink_path] + plink_imputesex_args
				LOGGER.debug("Performing Plink impute-sex")
				# plink_imputesex = execute_bash_command(plink_imputesex_cmd)
				[returncode, stdout, stderr] = execute_bash_command(cmd)
				# Get the output
				if returncode == 0:
					checksex_files_generated = os.listdir(plink_dir)
					LOGGER.info(f"Files available at '{plink_dir}': {checksex_files_generated}")
				else:
					LOGGER.error(f"Error {stderr} executing PLINK command: {cmd}")
					raise Exception("PLINK error")

				sexcheck_path = plink_output_prefix + '.sexcheck'
				if os.path.isfile(sexcheck_path) == False:
					LOGGER.error(f"File '{sexcheck_path}' does not exist")
				# 					LOGGER.error("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_checksex_path, plink_checksex[1], plink_checksex[2]))
				# 					raise Exception("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_checksex_path, plink_checksex[1], plink_checksex[2]))
				else:
					# Return method used and path to the .sexcheck output file generated by Plink
					return [method, sexcheck_path]
			else:
				LOGGER.error(f"Error {stderr} executing PLINK command: {cmd}")
				raise Exception("PLINK error")

	# Plot the inferred sex
	def plot_inferred_sex(self, title, sexcheck_fpath, ref_values_fpath, image_fpath):
		script_path = os.path.join(script_directory, "plot_inferred_sex.R")
		cmd = "Rscript " + script_path + " " + title + " " + sexcheck_fpath + " " + ref_values_fpath + " " + image_fpath
		LOGGER.info(f"{cmd}")
		result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
		# Get the output
		if result.returncode == 0:
			LOGGER.info(f"Inferred sex image at {image_fpath}")
		else:
			LOGGER.info(f"Error: {result.stderr}")

	def run(self):
		"""
		Execute the coverage based inferred sex analysis
		"""
		try:
			LOGGER.info('Starting %s', ANALYSIS_NAME)

			# Input files set up
			self.setup()

			# Retrieve sex information:
# 			individual_id = self.id_
			ind_metadata = self.get_individual_sex()

			LOGGER.info('ind_metadata = %s', ind_metadata)

			# Filter input VCF
			[filtered_vcf_path, filtered_vcf_name] = self.filter_variants()

			# Check files
			filtered_vars = self.variant_check(filtered_vcf_path, filtered_vcf_name)

			# Set thresholds for warning messages - NB, current thresholds are random - will need some testing
			if filtered_vars >= 1000:
				LOGGER.info("There are '{}' good quality chromosome X LD pruned variants after filtering".format(filtered_vars))
			elif filtered_vars >= 500 & filtered_vars < 1000:
				LOGGER.info("Warning: There are only '{}' good quality chromosome X LD pruned variants after filtering, results may be unreliable".format(filtered_vars))
			elif filtered_vars == 0:
				LOGGER.error("Warning: There are no chromosome X LD pruned variants after filtering")
			else:
				LOGGER.info("Warning: Poor quality data, there are only '{}' good quality chromosome X LD pruned variants after filtering, results will be unreliable".format(filtered_vars))

			method = None
			sexcheck_path = None
			if filtered_vars > 0:
				# Run inferred sex analysis:
				LOGGER.info('ind_metadata = %s', ind_metadata)
				if ind_metadata[self.executor["id_"]]['individualSex'] == 0:
					LOGGER.info("Running Plink impute-sex")
					[method, sexcheck_fpath] = self.impute_sex_plink(filtered_vcf_path)
				else:
					LOGGER.info("Running Plink check-sex")
					[method, sexcheck_fpath] = self.check_sex_plink(filtered_vcf_path)

				if os.path.isfile(sexcheck_fpath):
					self.inferred_sex_result.method = method
					self.inferred_sex_result.sampleId = self.executor["sample_ids"][0]
					self.inferred_sex_result.software = Software(name="PLINK",version="1.9")
					# self.inferred_sex_result.inferredKaryotypicSex =

					# Open and parse the sexcheck file
					with open(sexcheck_fpath, "r") as file:
						# Read the header line
						header = file.readline().strip().split()

						# Read the data line (only one line after the header)
						data_line = file.readline().strip()

						# If the data line is not empty, process it
						if data_line:
							# Split the line into columns
							columns = data_line.split()

							self.inferred_sex_result.values["FID"] = columns[0]
							self.inferred_sex_result.values["PEDSEX"] = columns[2]
							self.inferred_sex_result.values["SNPSEX"] = columns[3]
							self.inferred_sex_result.values["STATUS"] = columns[4]
							self.inferred_sex_result.values["F"] = columns[5]

					# Plot karyotypic sex
					image_fpath = os.path.join(self.output_dir, "inferred_sex.png")
					self.plot_inferred_sex(self.executor["id_"], sexcheck_fpath, self.sexcheck_ref_values_fpath, image_fpath)

					# Read the image file as binary data
					with open(image_fpath, "rb") as image_file:
						binary_data = image_file.read()

					# Encode the binary data as a Base64 string
					base64_string = base64.b64encode(binary_data).decode('utf-8')
					self.inferred_sex_result.images = [Image(name=f"Inferred Sex ({method})", base64=base64_string)]
					# 		self.inferred_sex_result.attributes =

					results_fpath = os.path.join(self.output_dir, "inferred_sex.json")
					LOGGER.debug('Generating JSON file with results. File path: "{}"'.format(results_fpath))
					with open(results_fpath, 'w') as file:
						json.dump(self.inferred_sex_result.model_dump(), file, indent=2)
					LOGGER.info('Finished writing JSON file with results: "{}"'.format(results_fpath))

			LOGGER.info('Complete successfully %s', ANALYSIS_NAME)

		except Exception as e:
			LOGGER.error("Error during %s: '%s'", ANALYSIS_NAME, format(e))
			raise







