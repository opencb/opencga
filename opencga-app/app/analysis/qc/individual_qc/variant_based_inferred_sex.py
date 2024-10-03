#!/usr/bin/env python3

import os
import logging
import gzip
import json

from utils import create_output_dir, execute_bash_command, generate_results_json
# something similar to Ximena's family_qc.relatedness_results?


LOGGER = logging.getLogger('variant_qc_logger')


class VariantBasedInferredSexAnalysis:
    def __init__(self, individual_qc_executor_info):
        """

        :param individual_qc_executor_info:
        """

        self.output_variant_based_inferred_sex_dir = None
        self.chrx_vars = None
        self.chrx_var_frq = None
        self.individual_qc_executor_info = individual_qc_executor_info
        # Results thingy similar to Ximena's?

    def variant_based_inferred_sex_setup(self):
        if self.individual_qc_executor_info != None:
            self.set_variant_based_inferred_sex_files()
            self.set_variant_based_inferred_sex_dir()
        else:
            msg = "No instance of IndividualQCExecutor was found. Therefore variant based inferred sex analysis cannot be executed."
            LOGGER.error(msg)
            raise TypeError(msg)

    def set_variant_based_inferred_sex_files(self):
        LOGGER.info("Checking and setting up variant based inferred sex files.")
        if os.path.exists(self.individual_qc_executor_info["resource_dir"]):
            check_sex_files = {
                "chrx_vars": os.path.join(self.individual_qc_executor_info["resource_dir"],"chrX_1000G_QC.prune.in"),
                # current prune.in input: 20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chrX.recalibrated_variants_filtered_annotated_chrX.prune.in
                "chrx_var_frq": os.path.join(self.individual_qc_executor_info["resource_dir"],"chrX_1000G_QC_prune_in.frq")
                # current frq input: 20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chrX.recalibrated_variants_filtered_annotated_chrX.frq
                }
            for key,file in check_sex_files.items():
                if os.path.isfile(file):
                    if key == "chrx_vars":
                        self.chrx_vars = file
                    else:
                        self.chr_var_frq = file
                    LOGGER.info("File '{}' set up successfully".format(file))
                else:
                    msg = "File '{}' does notxist".format(file)
                    LOGGER.error(msg)
                    raise FileNoFoundError(msg)
        else:
            msg = "Directory '{}' does not exist".format(resources_path)
            LOGGER.error(msg)
            raise FileNotFoundError(msg)

    def set_variant_based_inferred_sex_dir(self):
        check_sex_dir = create_output_dir(path_elements=[self.individual_qc_executor_info["output_parent_dir"],"variant_based_inferred_sex"])
        self.check_sex_dir = check_sex_dir

    def filter_variants(self):
        """
        Annotates input VCF with chromosome coordinate IDs and filters for pruned PASS chr X variants using BCFTools.
        :return:
        """
        # Read in VCF:
        vcf_file = gzip.open(self.individual_qc_executor_info["vcf_file"], "r")
        # Read in list of variants to filter for:
        good_vars = open(self.chrx_vars, "r")

        # Create output file:
        individual_id = self.individual_qc_executor_info["id_"]
        filtered_vcf_name = individual_id + self.sample_id + "_filtered_variants.vcf.gz"
        filtered_vcf_path = os.path.join(self.check_sex_dir, filtered_vcf_name)
        LOGGER.debug("Generating VCF filtered for good quality pruned chr X variants: '{}'".format(filtered_vcf_path))

        # Check if input VCF FILTER field contains PASS variants:
        pass_check_cmd = "bcftools view -i 'FILTER="PASS"' " + vcf_file + " zgrep -vw '#/|CHROM' | wc -l"
        pass_vars = execute_bash_command(cmd=pass_check_cmd)
        LOGGER.info("Checking for PASS variants in the FILTER field of '{}'".format(vcf_file))

        # Annotate input VCF with chr coordinate IDs, filter for chr X pruned variants and, if annotated, for PASS variants:
        annotate_and_filter_pass_cmd = "bcftools annotate --set-id '%CHROM\:%POS\:%REF\:%FIRST_ALT' " + vcf_file + "-Oz | bcftools view --include ID==@" + self.chrx_vars + " -Oz | bcftools view -i 'FILTER="PASS"' -Oz -o " + filtered_vcf_path
        annotate_and_filter_no_pass_cmd = "bcftools annotate --set-id '%CHROM\:%POS\:%REF\:%FIRST_ALT' " + vcf_file + "-Oz | bcftools view --include ID==@" + self.chrx_vars + " -Oz -o " + filtered_vcf_path
        #### NEED TO FIGURE OUT A WAY TO COUNT THE NUMBER OF VARIANTS REMAINING AFTER FILTERING SO I CAN HAVE AN ELIF FOR LOW NO. VARS
        if pass_vars == 0:
            execute_bash_command(cmd=annotate_and_filter_no_pass_cmd)
            LOGGER.debug("Annotating and filtering '{}' for all pruned chr X variants".format(vcf_file))
            LOGGER.info("WARNING: no FILTER information available, input data will not be filtered for PASS variants, results may be unreliable. There are '{}'")

            execute_bash_command(cmd=annotate_and_filter_pass_cmd)
            LOGGER.debug("Annotating and filtering '{}' for chr X pruned PASS variants".format(vcf_file))
            LOGGER.info("There are '{}' good quality chr X pruned variants after filtering".format(pass_vars))
        else:



        for sample in self.individual_qc_executor_info["sample_ids"]:
            sample_id = sample



