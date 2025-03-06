#!/usr/bin/env python3

import os
import logging
import json
import gzip
from utils import create_output_dir, execute_bash_command

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

#    def run(self):
        # Checking data
        # self.checking_data()  # TODO check input data

        # Running sample QC steps
        # self.step1()  # TODO run all necessary steps for this QC (e.g. relatedness)
        self.calculate_variant_based_inferred_sex()
        if self.bam_file != None:
            self.compute_coverage_metrics(bam_file=self.bam_file, output_dir=self.output_parent_dir)
            # check the output of the compute_coverage_metrics is OK and continue with the checks
            self.calculate_aneuploidies()
            self.calculate_coverage_based_inferred_sex()
        else:
            LOGGER.warning("Skipping coverage-based inferred sex: BIGWIG file not found for sample '" + sample_id + "'")
            pass
        # Return results
#        # ...  # TODO return results
        pass

#    def step1(self):
        # Create output dir for this step
        output_dir = create_output_dir([self.output_parent_dir, 'step1'])

        # Run step1
        # ...  # TODO execute this step commands
#        pass

    def compute_coverage_metrics(self, bam_file, output_dir):
        """
        Given a BigWig file, it calculates the average coverage per chromosome and autosomes
        :param bw_file: Path to the BigWig file for the sample
        :param output_dir: Output directory

        :return: dict with median coverage per chr1-22,X,Y and autosomes
        """
        # Create output dir for this step
        output_dir = create_output_dir([self.output_parent_dir, 'coverage_metrics'])

        bam_file = "file.bam"
        #cr = crpb.CountReadsPerBin([bam_file], binLength=50, stepSize=50)

    def calculate_aneuploidies(self, coverage, output_dir):
        # Create output dir for this step
        output_dir = create_output_dir([self.output_parent_dir, 'aneuploidies'])
        pass

    def calculate_coverage_based_inferred_sex(bw_file, config, coverage, output_dir):
        """
        Calculates inferred sex based on coverage if BAM information is available and checks if matches against the expected
        :param bw_file: Path to the BigWig file for the sample
        :param output_dir: Output directory
        :return:
        """
        chromosomes = ['chrX', 'chrY', "autosomes"]
        if all(chromosomes) in coverage and all(coverage[chromosomes]) is not None:
            # Calculate ratio X-chrom / autosomes
            ratio_chrX = float(coverage["chrX"] / coverage["autosomes"])
            # Calculate ratio Y-chrom / autosomes
            ratio_chrY = float(coverage["chrY"] / coverage["autosomes"])


        else:
            LOGGER.warning("Coverage-based inferred sex not calculate for sample {sample}. Missing or invalid coverage "
                           "values".format(sample=sample_ids))


    def filter_variants(self, chrx_vars, outdir_path):
        """
        Annotates input VCF with chromosome coordinate ID and filters for LD pruned PASS  chr X variants using BCFTools.
        :param self: Input data
        :param chrX_vars: List of LD pruned chr X variants
        :param outdir_path: Output directory path
        :return:
        """
        individual_id = self.id_
        # Read in input files:
        individual_info_json = json.load(self.info_file)
        vcf_file = gzip.open(self.vcf_file,'r')
        good_vars = open(chrx_vars, 'r')
        for sample in self.sample_ids:
            sample_id = sample
            # Create output directory:
            filtered_vcf_name = individual_id + "_" + sample_id + '_passed_variants.vcf.gz'
            filtered_vcf_path = os.path.join(str(outdir_path),filtered_vcf_name)

            # Annotate input VCF with chromosome coordinate ID:
            bcftools_annotate_cmd = "bcftools annotate --set-id '%CHROM\:%POS\:%REF\:%FIRST_ALT' " + vcf_file + " -Oz -o " + os.path.join(outdir_path, "/annotated.vcf.gz")
            execute_bash_command(cmd=bcftools_annotate_cmd)
            LOGGER.info("{file} annotated with chromosome coordinate IDs".format(file=vcf_file))

            # Filter for chromosome X LD pruned variants:
            bcftools_variant_filter_cmd = "bcftools view --include ID==@" + good_vars + " " + os.path.join(outdir_path, "/annotated.vcf.gz") + " -Oz -o " + os.path.join(outdir_path, "/varfilter.vcf.gz")
            execute_bash_command(cmd=bcftools_variant_filter_cmd)
            LOGGER.info("annotated.vcf.gz filtered for chr X LD pruned variants")

            # Check if FILTER field contains PASS variants:
            varcount_all_cmd = "bcftools view -i 'FILTER="PASS"' " + vcf_file + " zgrep -vw '#/|CHROM' | wc -l"
            all_pass_vars = execute_bash_command(cmd=varcount_all_cmd)
            LOGGER.info("checking for PASS variants in the FILTER field of '{}'".format(vcf_file))

            # Filter for PASS variants:
            bcftools_pass_filter_cmd = "bcftools view -i 'FILTER="PASS"' " + os.path.join(outdir_path, "/varfilter.vcf.gz") + " -Oz -o " + filtered_vcf_path
            execute_bash_command(cmd=bcftools_pass_filter_cmd)
            varcount_filt_cmd = "bcftools view -i 'FILTER="PASS"' " + filtered_vcf_path + " zgrep -vw '#\|CHROM' | wc -l"
            chrX_variants = execute_bash_command(cmd=varcount_filt_cmd)
            if all_pass_vars == 0:
                LOGGER.info("WARNING: no FILTER information available, input data will not be filtered for PASS variants, results may be unreliable. There are '{}' chr X LD pruned variants after filtering".format(chrX_variants))
            elif chrX_variants < 1000: # NB, need to come up with a good threshold - this is a random suggestion
                LOGGER.info("WARNING: poor quality data, results may be unreliable. There are '{}' good quality chr X LD pruned variants after filtering".format(chrX_variants))
            else:
                LOGGER.info("There are '{}' good quality chr X LD pruned variants after filtering")
            # Return annotated, filtered VCF path:
            return filtered_vcf_path
        pass


    def get_individual_sex(self):
        """
        Retrieve individual sex for sample and recode for Plink input.
        """
        individual_info_json = json.load(self.info_file)
        indsex = individual_info_json['sex']['id']
        karsex = individual_info_json['karyotypicSex']
        ind_metadata = {}
        for sample in self.sample_ids:
            ind_metadata[sample] =  {'individualId': individual_info_json['id'], 'individualSex': 0, 'sampleId': 'NA'}

            LOGGER.debug("Retrieve sex information and recode")
            for sam in individual_info_json['samples']:
                if sam['id'] in self.sample_ids:
                    ind_metadata[sam['id']]['sampleId'] = sample['id']
                    if indsex.upper() == 'MALE' or karsex == 'XY':
                        ind_metadata[sam['id']]['individualSex'] = 1
                    elif indsex.upper() == 'FEMALE' or karsex == 'XX':
                        ind_metadata[sam['id']]['individualSex'] = 2
                    else:
                        LOGGER.info("Sex information for Individual '{}' (Sample '{}') is not available, sex will be inferred".format(sam['id'],sample['id']))
        # Check individual information for each sample is present:
        for sample,individual_info in ind_metadata.items():
            if individual_info['individualSex'] == 0:
                LOGGER.warning("No individual information available for sample '{}'.".format(individual_info['sampleId']))
            else:
                LOGGER.info("Individual information for sample '{}' found".format(individual_info['sampleId']))
        # Return sex information:
        return ind_metadata
    pass


    def generate_plink_fam_file_input(self,output_dir):
        # Retrieve sex information:
        individual_id = self.id_
        ind_metadata = self.get_individual_sex()

        # Generate a text file to update the Plink fam file with sex information:
        sex_info_file_name = individual_id + '_sex_information.txt'
        sex_info_path = os.path.join(str(output_dir),sex_info_file_name)
        sex_info_input = open(sex_info_path,'w')
        LOGGER.debug("Generating text file to update the input sex information: '{}'".format(sex_info_path))

        for sample in self.sample_ids:
            # Individual information for sample:
            individual_info = ind_metadata[sample]
            # Plink sex update file format: familyId individualId sex (note, for the purpose of simplicity, individualId replaces familyId below)
            ###### BECAUSE IT'S POSSIBLE TO HAVE MULTIPLE FAMILY IDS & THE FAMILY ID ISN'T ACTUALLY REQUIRED FOR THE ANALYSIS
            sex_info_input.write(('/t'.join([sample,sample,individual_info['individualSex']])) + '\n')
            LOGGER.info("Text file generated to update the Plink fam file with sex information: '{}'".format(sex_info_path))

        # Return paths of text files generated:
        return sex_info_path
    pass


    def check_sex_plink(self,filtered_vcf_path,chrx_var_frq,plink_outdir,sex_method="Plink/check-sex"):
        # Define Plink check-sex methodology:
        LOGGER.info("Method: {}".format(sex_method))
        plink_outdir_path = create_output_dir(path_elements=[str(plink_outdir),"plink_check_sex"])
        sex_info = self.generate_plink_fam_file_input(output_dir=plink_outdir_path)

        # Plink parameters:
        file_prefix = self.id_ + "plink_sex_check_results"
        plink_output_prefix = os.path.join(plink_outdir,file_prefix)

        # Convert input to Plink format:
        plink_convert_input = ' '.join([plink,
                                        "--vcf", str(filtered_vcf_path),
                                        "--make-bed",
                                        "--keep-allele-order",
                                        "--update-sex", sex_info_path,
                                        "--split-x, "hg38", "no-fail",
                                        "--out", plink_output_prefix])
        LOGGER.debug("Generating Plink check-sex input files")
        plink_input_files = execute_bash_command(plink_convert_input)
        if plink_convert_input[0] == 0:
            plink_files_generated = os.listdir(plink_outdir)
            LOGGER.info("Files available: '{}':\n{}".format(plink_outdir,plink_files_generated))

            # Run Plink check-sex analysis:
            plink_check_sex_input = ' '.join([plink,
                                        "--bfile", plink_output_prefix,
                                        "--read-freq", str(chrx_var_freq),
                                        "--check-sex",
                                        "--out", plink_output_prefix])
            LOGGER.debug("Performing Plink check-sex")
            plink_sexcheck = execute_bash_command(plink_check_sex_input)
            if plink_check_sex_input[0] == 0:
                plink_files_generated = os.listdir(plink_outdir)
                LOGGER.info("Files available: '{}':\n{}".format(plink_outdir,plink_files_generated))

            plink_sexcheck_path = plink_output_prefix + '.sexcheck'
            if os.path.isfile(plink_sexcheck_path) == False:
                LOGGER.error("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_sexcheck_path,plink_sexcheck[1],plink_sexcheck[2]))
                raise Exception("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_sexcheck_path,plink_sexcheck[1],plink_sexcheck[2]))
            else:
                # Return method used and path of the output .sexcheck file generated by Plink
                return [sex_method,plink_sexcheck_path]
        pass


    def impute_sex_plink(self,filtered_vcf_path,chrx_var_frq,plink_outdir,sex_method="Plink/impute-sex"):
        # Define Plink impute-sex methodology:
        LOGGER.info("Method: {}".format(sex_method))
        plink_outdir_path = create_output_dir(path_elements=[str(plink_outdir),"plink_impute_sex"])

        # Plink parameters:
        file_prefix = self.id_ + "plink_impute_check_results"
        plink_output_prefix = os.path.join(plink_outdir,file_prefix)

        # Convert input to Plink format:
        plink_convert_input = ' '.join([plink,
                                        "--vcf", str(filtered_vcf_path),
                                        "--make-bed",
                                        "--keep-allele-order",
                                        "--split-x, "hg38", "no-fail",
                                        "--out", plink_output_prefix])
        LOGGER.debug("Generating Plink sex-check input files")
        plink_input_files = execute_bash_command(plink_convert_input)
        if plink_convert_input[0] == 0:
            plink_files_generated = os.listdir(plink_outdir)
            LOGGER.info("Files available: '{}':\n{}".format(plink_outdir,plink_files_generated))

            # Run Plink impute-sex analysis:
            plink_impute_sex_input = ' '.join([plink,
                                        "--bfile", plink_output_prefix,
                                        "--read-freq", str(chrx_var_frq),
                                        "--impute-sex",
                                        "--out", plink_output_prefix])
            LOGGER.debug("Performing Plink impute-sex")
            plink_imputesex = execute_bash_command(plink_impute_sex_input)
            if plink_impute_sex_input[0] == 0:
                plink_files_generated = os.listdir(plink_outdir)
                LOGGER.info("Files available: '{}':\n{}".format(plink_outdir,plink_files_generated))

            plink_sexcheck_path = plink_output_prefix + '.sexcheck'
            if os.path.isfile(plink_sexcheck_path) == False:
                LOGGER.error("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_sexcheck_path,plink_sexcheck[1],plink_sexcheck[2]))
                raise Exception("File '{}' does not exist. Check:\nSTDOUT: '{}'\nSTDERR: '{}'".format(plink_sexcheck_path,plink_sexcheck[1],plink_sexcheck[2]))
            else:
                # Return method used and path of the output .sexcheck file generated by Plink
                return [sex_method,plink_sexcheck_path]
        pass


    def calculate_variant_based_inferred_sex(self):
        """
        Run Plink check-sex or impute-sex as appropriate, dependent on the availability of user supplied sex information.
        :param self: Input data
        :filtered_vcf_path: Input VCF filtered for LD pruned chr X variants
        :param chrX_var_frq: 1000 Genomes population based allele frequencies for the selected LD pruned chr X variants
        :param plink_outdir: Output directory path
        :return:
        """
        # Reference file paths:
        chrx_vars = "resource_dir/20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chrX.recalibrated_variants_filtered_annotated_chrX.prune.in"
        chrx_var_frq = "resource_dir/20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chrX.recalibrated_variants_filtered_annotated_chrX.frq"

        # Create results output directory:
        output_dir = create_output_dir([self.output_parent_dir, 'variant_based_inferred_sex'])
        # Retrieve sex information:
        individual_id = self.id_
        ind_metadata = self.get_individual_sex()

        # Run inferred sex analysis:
        if ind_metadata['individualSex'] == 0:
            inferred_sex_output = impute_sex_plink(TO UPDATE)
            LOGGER.info("Running Plink impute-sex")
        else:
            inferred_sex_output = check_sex_plink(TO UPDATE)
            LOGGER.info("Running Plink check-sex")




    def plot_results(TO UPDATE):
        # Plot results:
        pass



    def inferred_sex_results_data_model(sex_method):
        inferredSex = [
            {
                method: "",
                sampleId: "",
                inferredKaryotypicSex "", #### I'M NOT SURE I LIKE THIS - I'D RATHER HAVE "inferredSex" - it's specific to the coverage based inference
                software: {
                    name: "plink",
                    version: "1.9",
                    commit: "",
                    params: {
                        key: value
                    }
                },
                values: {
                    "sampleId": "",
                    "PEDSEX": "",
                    "SNPSEX": "",
                    "STATUS": "",
                    "F": ""
                },
                images: [
                    {
                        name: "Sex check",
                        base64: "",
                        description: "TO UPDATE"
                    }
                ],
                attributes: {
                    cli: "",
                    files: [],
                    JOB_ID: ""
                }
            }
        ]
        if sex_method == "Plink/check-sex":
            LOGGER.info("Relatedness method used: '{}'".format(sex_method))
            inferredSex['method'] = str(sex_method)
        elif sex_method == "Plink/impute-sex":
            LOGGER.info("Relatedness method used: '{}'".format(sex_method))
            inferredSex['method'] = str(sex_method)
            inferredSex['values']['PEDSEX'] == "NA"
            inferredSex['values']['STATUS'] == "INFERRED"
#        elif sex_method == "MartaCoverageBasedSexMethodName":
#            LOGGER.info("Relatedness method used: '{}'".format(sex_method))
#            #etc
        else:
            LOGGER.warning("No method for sex inference defined.")
        # Return inferredSex data model json with method specific fields filled in:
        return inferredSex
    pass


