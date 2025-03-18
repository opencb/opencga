#!/usr/bin/env python3

import os
import logging
import gzip
import json
import pyBigWig
import subprocess
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
import sys

import individual_qc
from utils import *
from quality_control import *

LOGGER = logging.getLogger('variant_qc_logger')

ANALYSIS_NAME = "mendelian errors analysis"
ANALYSIS_PATH = "mendelian_errors"

# Get the directory of the script
script_directory = os.path.dirname(os.path.abspath(__file__))

class MendelianErrorsAnalysis:
    def __init__(self, executor):
        """
        :param executor:
        """

        self.name = ANALYSIS_NAME
        self.output_dir = create_output_dir(path_elements=[executor["output_parent_dir"], ANALYSIS_PATH])
        self.executor = executor
        self.mendelian_errors = individual_qc.MendelianErrors()

    def setup(self):
        if self.executor == None:
            msg = f"No instance of IndividualQCExecutor was found. Therefore {ANALYSIS_NAME} cannot be executed."
            LOGGER.error(msg)
            raise TypeError(msg)

    # Plot the karyotypic sex
    def plot_mendelian_errors(self, input_fpath, image_fpath):
        script_path = os.path.join(script_directory, "plot_mendelian_errors.R")
        cmd = "Rscript " + script_path + " " + input_fpath + " " + image_fpath
        LOGGER.info(f"Command: {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        # Get the output
        if result.returncode == 0:
            LOGGER.info(f"Mendelian errors image at {image_fpath}")
        else:
            LOGGER.info(f"Error: {result.stderr}")

    def parse_mendel_output(self, mendel_fpath):
        individual_id = self.executor["id_"]
        sample_id = get_sample_id_from_individual_id(individual_id, self.executor["samples_info"])

        # Nested dictionary: {chromosome: {error_type: count}}
        chromosome_error_counts = defaultdict(lambda: defaultdict(int))
        total_chromosome_counts = defaultdict(int)  # Total errors per chromosome

        # Read and process the file
        with open(mendel_fpath, "r") as file:
            header_line = True
            for line in file:
                if header_line:
                    header_line = False
                    continue
                if line.startswith("#"):  # Skip header if present
                    continue
                columns = line.strip().split()  # Split by whitespace
                if len(columns) < 5:
                    continue  # Skip malformed lines

                chromosome = columns[2]  # Assuming chromosome is in column 3 (index 2)
                error_type = columns[4]  # Assuming error type is in column 5 (index 4)

                # Increment nested count
                chromosome_error_counts[chromosome][error_type] += 1
                # Count total errors per chromosome
                total_chromosome_counts[chromosome] += 1

        # Compute total of errors
        total_errors = sum(total_chromosome_counts.values())

        # Compute chromosome error ratios
        chromosome_ratios = {chrom: count / total_errors for chrom, count in total_chromosome_counts.items()}

        # Sort chromosomes by total errors (descending)
        sorted_chromosomes = sorted(total_chromosome_counts.keys(), key=lambda c: total_chromosome_counts[c], reverse=True)

        sample_mendelian_errors = individual_qc.SampleMendelianErrors(sample=sample_id, numErrors=total_errors, errorCodeAggregation=[])
        # Set SampleMendelianErrors
        for chrom in sorted_chromosomes:
            chromosome_aggregation = individual_qc.ChromomeSampleMendelianErrors(
                chromosome=chrom,
                numErrors=total_chromosome_counts[chrom],
                ratio=chromosome_ratios[chrom],
                errorCodeAggregation=dict(sorted(chromosome_error_counts[chrom].items(), key=lambda x: x[1], reverse=True)))
            sample_mendelian_errors.chromAggregation.append(chromosome_aggregation)

        self.mendelian_errors.numErrors = total_errors
        self.mendelian_errors.sampleAggregation = [sample_mendelian_errors]

        # Images
        sample_mendelian_errors_fpath = mendel_fpath + ".json"
        with open(sample_mendelian_errors_fpath, 'w') as file:
            json.dump(sample_mendelian_errors.model_dump(), file, indent=2)
        image_fpath = mendel_fpath + ".png"
        self.plot_mendelian_errors(sample_mendelian_errors_fpath, image_fpath)
        base64_string = get_base64_image(image_fpath)
        self.mendelian_errors.images = [Image(name=f"Mendelian errors (PLINK/mendel)", base64=base64_string)]

        # self.mendelian_errors.attributes =

    # Function to infer sex from the coverage (i.e., using a BIGWIG file)
    def calculate_mendelian_errors(self):
        """
        Calculates inferred sex based on coverage if BAM information is available and checks if matches against the expected
        :return:
        """

        plink_path="plink1.9"
        vcf_file = self.executor["vcf_file"]
        family_id = get_family_id(self.executor["samples_info"])

        # Create a directory for PLINK if it does not already exist:
        plink_dir = create_output_dir(path_elements=[self.output_dir, 'plink_mendelian_errors'])

        # Prepare PLINK files
        individual_id = self.executor["id_"]
        file_prefix =  individual_id + "_plink_mendel_results"
        plink_prefix = os.path.join(plink_dir, file_prefix)

        sex_fpath = create_sex_file(plink_dir, self.executor["samples_info"])
        parents_fpath = create_parents_file(plink_dir, self.executor["samples_info"])
        phenotype_fpath = create_phenotype_file(plink_dir, self.executor["samples_info"])

        cmd = f"{plink_path} --vcf {vcf_file} --make-bed --const-fid {family_id} --allow-extra-chr --update-sex {sex_fpath} --update-parents {parents_fpath} --pheno {phenotype_fpath} --out {plink_prefix}"
        LOGGER.debug("Performing PLINK make-bed")
        execute_bash_command(cmd)
        # Get the output
        files_generated = os.listdir(plink_dir)
        LOGGER.info(f"Files available at '{plink_dir}': {files_generated}")

        # Excute PLINK mendel
        cmd = f"{plink_path} --bfile {plink_prefix} --mendel --allow-extra-chr --out {plink_prefix}"
        LOGGER.debug("Performing PLINK mendel")
        execute_bash_command(cmd)
        # Get the output
        files_generated = os.listdir(plink_dir)
        LOGGER.info(f"Files available at '{plink_dir}': {files_generated}")

        # Parse mendel output
        mendel_fpath = plink_prefix + '.mendel'
        if os.path.isfile(mendel_fpath) == False:
            msg = f"File '{mendel_fpath}' does not exist"
            LOGGER.error(msg)
            raise Exception(msg)
        else:
            # Parse PLINK mendel file
            self.parse_mendel_output(mendel_fpath)

    def run(self):
        """
        Execute the coverage based inferred sex analysis
        """
        try:
            LOGGER.info('Starting %s', ANALYSIS_NAME)

            # Input files set up
            self.setup()

            # Performing mendelian errors analysis from PLINK
            self.calculate_mendelian_errors()

            LOGGER.info('Complete successfully %s', ANALYSIS_NAME)
        except Exception as e:
            LOGGER.error("Error during %s: '%s'", ANALYSIS_NAME, format(e))
            raise
