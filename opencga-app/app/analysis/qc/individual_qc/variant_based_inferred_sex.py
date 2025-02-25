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
        for sample_id in self.individual_qc_executor_info["sample_ids"]:
            individual_id = self.individual_qc_executor_info["id_"]
            filtered_vcf_name = individual_id + sample_id + "_filtered_variants.vcf.gz"
            filtered_vcf_path = os.path.join(self.check_sex_dir, filtered_vcf_name)
            LOGGER.debug("Generating VCF filtered for good quality pruned chr X variants: '{}'".format(filtered_vcf_path))

            # Check if input VCF FILTER field contains PASS variants:
            pass_check_cmd = "bcftools view -i 'FILTER="PASS"' " + vcf_file + " zgrep -vw '#/|CHROM' | wc -l"
            pass_vars = execute_bash_command(cmd=pass_check_cmd)
            LOGGER.info("Checking for PASS variants in the FILTER field of '{}'".format(vcf_file))

            # Annotate input VCF with chr coordinate IDs, filter for chr X pruned variants and, if annotated, for PASS variants:
            annotate_and_filter_pass_cmd = "bcftools annotate --set-id '%CHROM\:%POS\:%REF\:%FIRST_ALT' " + vcf_file + "-Oz | bcftools view --include ID==@" + self.chrx_vars + " -Oz | bcftools view -i 'FILTER="PASS"' -Oz -o " + filtered_vcf_path
            annotate_and_filter_no_pass_cmd = "bcftools annotate --set-id '%CHROM\:%POS\:%REF\:%FIRST_ALT' " + vcf_file + "-Oz | bcftools view --include ID==@" + self.chrx_vars + " -Oz -o " + filtered_vcf_path
            if pass_vars == 0:
                execute_bash_command(cmd=annotate_and_filter_no_pass_cmd)
                LOGGER.debug("Annotating and filtering '{}' for all pruned chr X variants".format(vcf_file))
                LOGGER.info("WARNING: no FILTER information available, input data will not be filtered for PASS variants, results may be unreliable")
            else:
                execute_bash_command(cmd=annotate_and_filter_pass_cmd)
                LOGGER.debug("Annotating and filtering '{}' for chr X pruned PASS variants".format(vcf_file))

            # Return filtered VCF path:
            return [filtered_vcf_path, filtered_vcf_name]

    def variant_check(self, filtered_vcf_path, filtered_vcf_name):
        # Calculate number of variants remaining for Plink check-sex/impute-sex after all filtering
        variant_check_cmd = "zgrep -vw '#/|CHROM' " + filtered_vcf_path + " | wc -l"
        filtered_vars = execute_bash_command(cmd=variant_check_cmd)
        LOGGER.info("Checking final number of variants in {}".format(filtered_vcf_name))

        # Set thresholds for warning messages - NB, current thresholds are random - will need some testing
        if filtered_vars >= 1000:
            LOGGER.info("There are '{}' good quality chromosome X LD pruned variants after filtering".format(filtered_vars))
        elif filtered_vars >= 500 & filtered_vars < 1000:
            LOGGER.info("WARNING: There are only '{}' good quality chromosome X LD pruned variants after filtering, results may be unreliable".format(filtered_vars))
        else:
            LOGGER.info("WARNING: Poor quality data, there are only '{}' good quality chromosome X LD pruned variants after filtering, results will be unreliable".format(filtered_vars))

    def get_individual_sex(self):
        """
        Retrieve individual sex for sample if it is available, and recode for Plink input.
        :return:
        """
        individual_info = open(self.individual_qc_executor_info["info_file"])
        individual_info_json = json.load(individual_info)
        indsex = individual_info_json['sex']['id']
        karsex = individual_info_json['karyotypicSex']
        ind_metadata = {}
        for sample in self.individual_qc_executor_info["sample_ids"]:
            ind_metadata[sample] = {'individualId': individual_info_json['id'], 'individualSex': 0, 'sampleId': 'NA'}

            LOGGER.debug("Retrieve sex information and recode")
            for sam in individual_info_json['samples']:
                if sam['id'] in self.individual_qc_executor_info["sample_ids"]:
                    ind_metadata[sam['id']]['sampleId'] = sample['id']
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
        individual_id = self.individual_qc_executor_info["id_"]
        ind_metadata = self.get_individual_sex()

        # Create a directory for Plink if it does not already exist:
        plink_dir = create_output_dir(path_elements[self.check_sex_dir, 'plink_check-sex'])

        # Generate a text file to update the Plink fam file with sex information:
        sex_info_file_name = individual_id + '_sex_information.txt'
        sex_info_path = os.path.join(plink_dir, sex_info_file_name)
        sex_info_input = open(sex_info_path, 'w')
        LOGGER.debug("Generating text file to update the Plink input sex information: '{}'".format(sex_info_path))

        for sample in self.individual_qc_executor_info["sample_ids"]:
            # Individual sample information:
            individual_info = ind_metadata[sample]
            # Plink sex update file format: familyId individualId sex
            sex_info_input.write(('/t'.join([sample,sample,individual_info['individualSex']])) + '\n')
            # Note, for the purpose of this function, individualId replaces familyId to prevent issues where an individual belongs to more than one family
            LOGGER.info("Test file generated to update the Plink fam file with sex information: '{}'".format(sex_info_path))

        # Return paths of text files generated:
        return sex_info_path

    def check_sex_plink(self, filtered_vcf_path, plink_path="plink1.9"):
        method = "Plink/check-sex"
        # Define Plink check-sex methodology:
        LOGGER.info("Method: {}".format(method))

        # Create a directory for Plink if it does not already exist:
        plink_dir = create_output_dir(path_elements[self.check_sex_dir, 'plink_check-sex'])
        sex_info = self.generate_plink_fam_file_input()

        # Convert input to Plink format:
        plink_path = str(plink_path)
        for sample_id in self.individual_qc_executor_info["sample_ids"]:
            file_prefix = self.individual_qc_executor_info["id_"] + sample_id + "_plink_check-sex_results"
            plink_output_prefix = os.path.join(plink_dir, file_prefix)
            plink_convert_args = ["--vcf", str(filtered_vcf_path),
                                  "--make-bed",
                                  "--keep-allele-order",
                                  "--update-sex", sex_info_path,
                                  "--split-x", "hg38", "no-fail",
                                  "--out", plink_output_prefix]
            plink_convert_cmd = [plink_path] + plink_convert_args
            LOGGER.debug("Generating Plink check-sex input files")
            plink_input_files = execute_bash_command(plink_convert_cmd)
            if plink_convert_args[0] == 0:
                plink_files_generated = os.listdir(plink_dir)
                LOGGER.info("Files available: '{}':\n{}".format(plink_dir, plink_files_generated))

                # Run Plink check-sex analysis:
                plink_checksex_args = ["--bfile", plink_output_prefix,
                                       "--read-freq", str(self.chrx_var_frq),
                                       "--check-sex",
                                       "--out", plink_output_prefix]
                plink_checksex_cmd = [plink_path] + plink_checksex_args
                LOGGER.debug("Performing Plink check-sex")
                plink_checksex = execute_bash_command(plink_checksex_cmd)
                if plink_checksex_args[0] == 0:
                    checksex_files_generated = os.listdir(plink_dir)
                    LOGGER.info("Files available: '{}':\n{}".format(plink_dir, checksex_files_generated))

                plink_checksex_path = plink_output_prefix + '.sexcheck'
                if os.path.isfile(plink_checksex_path) == False:
                    LOGGER.error("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_checksex_path, plink_checksex[1], plink_checksex[2]))
                    raise Exception("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_checksex_path, plink_checksex[1], plink_checksex[2]))
                else:
                    # Return method used and path to the .secheck output file generated by Plink
                    return [method, plink_checksex]

    def impute_sex_plink(self, filtered_vcf_path, plink_path="plink1.9"):
        method = "Plink/impute-sex"
        # Define Plink impute-sex methodology:
        LOGGER.info("Method: {}".format(method))

        # Create a directory for Plink if it does not already exist:
        plink_dir = create_output_dir(path_elements[self.check_sex_dir, 'plink_impute-sex'])

        # Convert input to Plink format:
        plink_path = str(plink_path)
        for sample_id in self.individual_qc_executor_info["sample_ids"]:
            file_prefix = self.individual_qc_executor_info["id_"] + sample_id + "_plink_impute_sex_results"
            plink_output_prefix = os.path.join(plink_dir, file_prefix)
            plink_convert_args = ["--vcf", str(filtered_vcf_path),
                                  "--make-bed",
                                  "--keep-allele-order",
                                  "--split-x", "hg38", "no-fail",
                                  "--out", plink_output_prefix]
            plink_convert_cmd = [plink_path] + plink_convert_args
            LOGGER.debug("Generating Plink impute-sex input files")
            plink_input_files = execute_bash_command(plink_convert_cmd)
            if plink_convert_args[0] == 0:
                plink_files_generated = os.listdir(plink_dir)
                LOGGER.info("Files available: '{}':\n{}".format(plink_dir, plink_files_generated))

                # Run Plink impute-sex analysis:
                plink_imputesex_args = ["--bfile", plink_output_prefix,
                                       "--read-freq", str(self.chrx_var_frq),
                                       "--impute-sex",
                                       "--out", plink_output_prefix]
                plink_imputesex_cmd = [plink_path] + plink_imputesex_args
                LOGGER.debug("Performing Plink impute-sex")
                plink_imputesex = execute_bash_command(plink_imputesex_cmd)
                if plink_imputesex_args[0] == 0:
                    imputesex_files_generated = os.listdir(plink_dir)
                    LOGGER.info("Files available: '{}':\n{}".format(plink_dir, imputesex_files_generated))

                plink_imputesex_path = plink_output_prefix + '.sexcheck'
                if os.path.isfile(plink_imputesex_path) == False:
                    LOGGER.error("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_imputesex_path, plink_imputesex[1], plink_imputesex[2]))
                    raise Exception("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_imputesex_path, plink_imputesex[1], plink_imputesex[2]))
                else:
                    # Return method used and path to the .secheck output file generated by Plink
                    return [method, plink_imputesex]








