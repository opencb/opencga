#!/usr/bin/env python3

import os
import logging
import gzip
import json

import common
from utils import *
from quality_control import *

LOGGER = logging.getLogger('variant_qc_logger')

ANALYSIS_NAME = "relatedness analysis"
ANALYSIS_PATH = "relatedness"

# Get the directory of the script
script_directory = os.path.dirname(os.path.abspath(__file__))


class RelatednessAnalysis:
    def __init__(self, executor):
        """
        """

        self.name = ANALYSIS_NAME
        self.output_dir = create_output_dir(path_elements=[executor["output_parent_dir"], ANALYSIS_PATH])

        self.prune_in_file = None
        self.pop_freq_file = None
        self.pop_exclude_var_file = None
        self.relatedness_thresholds_file = None
        self.executor = executor
        self.relatedness = common.Relatedness()

    def relatedness_setup(self):
        if self.executor != None:
            self.set_relatedness_files()
        else:
            msg = "No instance of FamilyQCExecutor was found. Therefore relatedness analysis cannot be executed."
            LOGGER.error(msg)
            raise TypeError(msg)

    def set_relatedness_files(self):
        LOGGER.info('Checking and setting up relatedness files')
        if os.path.exists(self.executor["resource_dir"]):
            relatedness_files = {
                "prune_in_file": os.path.join(self.executor["resource_dir"], RELATEDNESS_PRUNE_IN_FILE),
                "pop_freq_file": os.path.join(self.executor["resource_dir"], RELATEDNESS_PRUNE_IN_FREQS_FILE),
                "pop_exclude_var_file": os.path.join(self.executor["resource_dir"], RELATEDNESS_PRUNE_OUT_MARKERS_FILE),
                "relatedness_thresholds_file": os.path.join(self.executor["resource_dir"], RELATEDNESS_THRESHOLDS_FILE)
                }
            for key,file in relatedness_files.items():
                if os.path.isfile(file):
                    if key == "prune_in_file":
                        self.prune_in_file = file
                    elif key == "pop_freq_file":
                        self.pop_freq_file = file
                    elif key == "pop_exclude_var_file":
                        self.pop_exclude_var_file = file
                    else:
                        self.relatedness_thresholds_file = file
                    LOGGER.info('File {} set up successfully'.format(file))
                else:
                    msg = 'File "{}" does not exist'.format(file)
                    LOGGER.error(msg)
                    raise FileNotFoundError(msg)
        else:
            msg = 'Directory "{}" does not exist'.format(resources_path)
            LOGGER.error(msg)
            raise FileNotFoundError(msg)

    def filter_variants(self):
        """
        Annotates input VCF with chromosome coordinate IDs and filters for pruned PASS chr X variants using BCFTools.
        :return:
        """
        # Input VCF file
        vcf_file = self.executor["vcf_file"]

        meta_id = self.executor["id_"]
        filtered_vcf_name = meta_id + "_filtered_variants.vcf.gz"
        filtered_vcf_path = os.path.join(self.output_dir, filtered_vcf_name)
        LOGGER.debug("Generating VCF filtered for good quality variants: '{}'".format(filtered_vcf_path))

        # Check if input VCF FILTER field contains PASS variants:
        LOGGER.info(f"Checking for PASS variants in the FILTER field of {vcf_file}")
        cmd = f"bcftools view -i 'FILTER=\"PASS\"' {vcf_file} | grep -v '^#\|CHROM' | wc -l"
        cmd_result = execute_bash_command(cmd)
        pass_vars = int(cmd_result[1].strip())
        LOGGER.info(f"Number of PASS variants: {pass_vars}")

        # Annotate input VCF with chr coordinate IDs, filter for chr X pruned variants and, if annotated, for PASS variants:
        if pass_vars == 0:
            LOGGER.debug("Annotating and filtering '{}' for all pruned chr X variants".format(vcf_file))
            LOGGER.info("WARNING: no FILTER information available, input data will not be filtered for PASS variants, results may be unreliable")
            cmd = f"bcftools annotate --set-id '%CHROM\:%POS\:%REF\:%FIRST_ALT' {vcf_file} -Oz | bcftools view --include ID==@{self.prune_in_file} -Oz -o {filtered_vcf_path}"
            LOGGER.info(cmd)
            execute_bash_command(cmd)
        else:
            LOGGER.debug("Annotating and filtering '{}' for chr X pruned PASS variants".format(vcf_file))
            cmd = f"bcftools annotate --set-id '%CHROM\:%POS\:%REF\:%FIRST_ALT' {vcf_file} -Oz | bcftools view --include ID==@{self.prune_in_file} -Oz | bcftools view -i 'FILTER=\"PASS\"' -Oz -o {filtered_vcf_path}"
            LOGGER.info(cmd)
            execute_bash_command(cmd)

        # Return filtered VCF path:
        return filtered_vcf_path

    # def filter_rename_variants_vcf(self):
    #     # Reading VCF
    #     vcf_fhand = gzip.open(self.executor["vcf_file"], 'r')
    #     # Reading pop_freq file
    #     input_pop_freq_fhand = open(self.pop_freq_file, 'r')
    #     LOGGER.info('Getting variant IDs to include in the VCF from file: "{}"'.format(self.pop_freq_file))
    #     variant_ids_to_include = [line.strip().split()[1] for line in input_pop_freq_fhand]
    #
    #     # Create output dir and file
    #     filtered_vcf_outdir_fpath = create_output_dir(path_elements=[self.output_relatedness_dir, 'filtered_vcf'])
    #     output_file_name = 'filtered_vcf_' + os.path.basename(self.executor["vcf_file"])
    #     filtered_vcf_fpath = os.path.join(filtered_vcf_outdir_fpath, output_file_name)
    #     filtered_vcf_fhand = gzip.open(filtered_vcf_fpath, 'wt')
    #     LOGGER.info('Generating filtered VCF with variant IDs under ID column: "{}"'.format(filtered_vcf_fpath))
    #
    #     # Generate VCF with variant IDs under ID column
    #     for line in vcf_fhand:
    #         line = line.decode()
    #         if line.startswith('#'):
    #             # Writing VCF header as it is
    #             filtered_vcf_fhand.write(line)
    #             continue
    #         else:
    #             # Getting variant data
    #             variant_items = line.strip().split()
    #             variant_id = ':'.join([variant_items[0], variant_items[1], variant_items[3], variant_items[4]])
    #             if 'chr' not in variant_items[0]:
    #                 variant_id = 'chr' + variant_id
    #             if variant_id in variant_ids_to_include:
    #                 variant_items[2] = variant_id
    #                 filtered_vcf_fhand.write('\t'.join(variant_items) + '\n')
    #     LOGGER.info('Filtered VCF with variant IDs under ID column generated: "{}"'.format(filtered_vcf_fpath))
    #
    #     # Return filtered VCF path (variant IDs under ID column)
    #     return filtered_vcf_fpath

    def get_samples_individuals_info(self):
        family_info_fhand = open(self.executor["info_file"])
        family_info_json = json.load(family_info_fhand)
        samples_individuals = {}
        for sample in self.executor["sample_ids"]:
            samples_individuals[sample] = {'individualId': '', 'individualSex': 0, 'fatherId': 'NA', 'motherId': 'NA',
                                           'familyMembersRoles': 'NA'}

        LOGGER.info('Getting individual information for each sample')
        for member in family_info_json['members']:
            for sample_member in member['samples']:
                if sample_member['id'] in self.executor["sample_ids"]:
                    # Filling in individual info
                    LOGGER.info('Individual information for sample "{}" found'.format(sample_member['id']))
                    samples_individuals[sample_member['id']]['individualId'] = member['id']
                    if (member['sex']['id']).upper() == 'MALE' or member['karyotypicSex'] == 'XY':
                        samples_individuals[sample_member['id']]['individualSex'] = 1
                    elif (member['sex']['id']).upper() == 'FEMALE' or member['karyotypicSex'] == 'XX':
                        samples_individuals[sample_member['id']]['individualSex'] = 2
                    else:
                        LOGGER.warning(
                            'Sex information for individual "{}" (sample "{}") is not available. Hence, sex code for the fam file will be 0.'.format(
                                member['id'], sample_member['id']))
                        pass
                    # Filling in father info
                    if 'id' in member['father'].keys():
                        samples_individuals[sample_member['id']]['fatherId'] = member['father']['id']
                    # Filling in mother info
                    if 'id' in member['mother'].keys():
                        samples_individuals[sample_member['id']]['motherId'] = member['mother']['id']
                    # Filling in family roles info for the individual
                    samples_individuals[sample_member['id']]['familyMembersRoles'] = family_info_json['roles'][
                        member['id']]

        # Checking if individual information for each sample was found
        for sample, individual_info in samples_individuals.items():
            if individual_info['individualId'] == '':
                LOGGER.warning('No individual information available for sample "{}".'.format(sample['id']))
            else:
                LOGGER.info('Individual information for sample "{}" found'.format(sample_member['id']))

        # Return samples_individuals dictionary
        return samples_individuals

    def generate_files_for_plink_fam_file(self):
        # Getting family id and sample_individuals_info
        family_id = self.executor["id_"]
        samples_individuals = self.get_samples_individuals_info()

        # Create dir for PLINK if it does not exist yet:
        plink_dir = create_output_dir(path_elements=[self.output_dir, 'plink_IBD'])

        # Generating text file to update sex information
        sex_information_output_file_name = family_id + '_individual_sample_sex_information.txt'
        sex_information_output_fpath = os.path.join(plink_dir, sex_information_output_file_name)
        sex_information_output_fhand = open(sex_information_output_fpath, 'w')
        LOGGER.info('Generating text file to update individual, sample, sex information: "{}"'.format(
            sex_information_output_fpath))

        # Generating text file to update parent-offspring relationships
        parent_offspring_output_file_name = family_id + '_parent_offspring_relationships.txt'
        parent_offspring_output_fpath = os.path.join(plink_dir, parent_offspring_output_file_name)
        parent_offspring_output_fhand = open(parent_offspring_output_fpath, 'w')
        LOGGER.info('Generating text file to update parent-offspring relationships: "{}"'.format(parent_offspring_output_fpath))

        for sample in self.executor["sample_ids"]:
            # Individual information for that sample
            individual_info = samples_individuals[sample]
            # Structure = FamilyID SampleID Sex
            sex_information_output_fhand.write(
                ('\t'.join([family_id, sample, str(individual_info['individualSex'])])) + '\n')
            # Structure = FamilyID SampleID FatherID MotherID
            parent_offspring_info = [family_id, sample, str(0), str(0)]
            father_id = individual_info['fatherId']
            mother_id = individual_info['motherId']
            if father_id != 'NA':
                for sample_id, individual_info in samples_individuals.items():
                    if individual_info['individualId'] == father_id:
                        parent_offspring_info[2] = sample_id
                        break
            if mother_id != 'NA':
                for sample_id, individual_info in samples_individuals.items():
                    if individual_info['individualId'] == mother_id:
                        parent_offspring_info[3] = sample_id
                        break
            parent_offspring_output_fhand.write(('\t'.join(parent_offspring_info)) + '\n')
        LOGGER.info('Text file generated to update individual, sample, sex information: "{}"'.format(
            sex_information_output_fpath))
        LOGGER.info(
            'Text file generated to update parent-offspring relationships: "{}"'.format(parent_offspring_output_fpath))

        # Return paths of text files generated. First path: individual, sample, sex information file. Second path: parent-offspring information file.
        return [sex_information_output_fpath, parent_offspring_output_fpath]

    def relatedness_plink(self, filtered_vcf_fpath, plink_path="plink1.9"):
        method = "PLINK/IBD"
        LOGGER.info('Method: {}'.format(method))

        family_id = get_family_id(self.executor["samples_info"])
        individual_id = self.executor["id_"]

        plink_dir = create_output_dir(path_elements=[self.output_dir, 'plink_IBD'])

        sex_fpath = create_sex_file(plink_dir, self.executor["samples_info"])
        parents_fpath = create_parents_file(plink_dir, self.executor["samples_info"])
        phenotype_fpath = create_phenotype_file(plink_dir, self.executor["samples_info"])

        # Preparing PLINK commands
        file_prefix =  individual_id + "_plink_relatedness_results"
        plink_prefix = os.path.join(plink_dir, file_prefix)

        cmd = f"{plink_path} --vcf {filtered_vcf_fpath} --make-bed --const-fid {family_id} --chr '1-22' --not-chr 'X,Y,MT' --allow-extra-chr --snps-only --biallelic-only strict --vcf-half-call haploid --update-sex {sex_fpath} --update-parents {parents_fpath} --pheno {phenotype_fpath} --out {plink_prefix}"
        LOGGER.info('Generating PLINK files (--make-bed)')
        execute_bash_command(cmd)
        files_generated = os.listdir(plink_dir)
        LOGGER.info('Files available in directory "{}":\n{}'.format(plink_dir, files_generated))

        cmd = f"{plink_path} --bfile {plink_prefix} --genome rel-check --read-freq {self.pop_freq_file} --exclude {self.pop_exclude_var_file} --out {plink_prefix}"
        LOGGER.info("Performing IBD analysis")
        execute_bash_command(cmd)
        files_generated = os.listdir(plink_dir)
        LOGGER.info('Files available in directory "{}":\n{}'.format(plink_dir, files_generated))

        plink_genome_fpath = plink_prefix + '.genome'
        if os.path.isfile(plink_genome_fpath) == False:
            msg = f"File '{plink_genome_fpath}' does not exist"
            LOGGER.error(msg)
            raise Exception(msg)

        # Filling in method, software, and attributes fields from the relatedness results data model
        LOGGER.info('Filling in method, software, and attributes fields from the relatedness results data model')
        self.relatedness.method = method
        self.relatedness.software = Software(name="PLINK", version="1.9", commit="", params={})

        # Return PLINK genome file
        return plink_genome_fpath

    @staticmethod
    def relatedness_validation(reported_result, inferred_result):
        LOGGER.info('Comparing reported {} and inferred {}'.format(reported_result, inferred_result))
        if 'UNKNOWN' == reported_result or reported_result == "":
            validation = "UNKNOWN"
        else:
            reported_result = set(reported_result.split(', '))
            inferred_result = set(inferred_result.split(', '))
            if reported_result == inferred_result or reported_result.issubset(inferred_result):
                validation = "PASS"
            else:
                validation = "FAIL"

        # Return validation result
        return validation

    def relatedness_inference(self, sampleId1, sampleId2, score_values):
        # Reading relatedness thresholds file (.tsv)
        LOGGER.info('Getting relatedness thresholds from file: "{}"'.format(self.relatedness_thresholds_file))
        relatedness_thresholds_fhand = open(self.relatedness_thresholds_file)
        relationship_groups_thresholds_dict = {}
        for index, line in enumerate(relatedness_thresholds_fhand):
            relatedness_thresholds_row_values = line.strip().split()
            if index == 0:
                relatedness_thresholds_file_header = relatedness_thresholds_row_values
                continue
            for column, value in enumerate(relatedness_thresholds_row_values):
                if relatedness_thresholds_file_header[column] == 'relationship':
                    relationship_key = value
                    relationship_groups_thresholds_dict[relationship_key] = {}
                else:
                    relationship_groups_thresholds_dict[relationship_key][relatedness_thresholds_file_header[column]] = float(value)

        # Inferring family relationship block:
        LOGGER.info("Inferring family relationship between '{}' and '{}' ".format(sampleId1, sampleId2))
        inference_groups = []
        for relationship, values in relationship_groups_thresholds_dict.items():
            # Check if PI_HAT, Z0, Z1, Z2 values (from PLINK .genome file) are within range (internal thresholds)
            if ((values['minPiHat']) <= score_values["PI_HAT"] <= values['maxPiHat']) and (
                    values['minZ0'] <= score_values["z0"] <= values['maxZ0']) and (
                    values['minZ1'] <= score_values["z1"] <= values['maxZ1']) and (
                    values['minZ2'] <= score_values["z2"] <= values['maxZ2']):
                inference_groups.append(str(relationship))
                continue
        if len(inference_groups) == 0:
            inferred_relationship = "UNKNOWN"
            LOGGER.info("UNKNOWN family relationship inferred between '{}' and '{}' ".format(sampleId1, sampleId2))
        else:
            inferred_relationship = ', '.join(inference_groups)
        LOGGER.info("Family relationship inferred between '{}' and '{}' ".format(sampleId1, sampleId2))
        
        return inferred_relationship

    # @staticmethod
    # def relatedness_report(samples_individuals_info):
    #     # Getting reported family relationship block:
    #     expected_keys = {"sampleId1","individualInfo1","sampleId2","individualInfo2"}
    #     if expected_keys != set(samples_individuals_info.keys()):
    #         msg = "Expected keys {} are not present in samples_individuals_info dictionary. Keys found: {}".format(expected_keys, samples_individuals_info.keys())
    #         LOGGER.error(msg)
    #         raise TypeError(msg)
    #     LOGGER.info('Getting reported relatedness information for sample {} and sample {}'.format(samples_individuals_info["sampleId1"], samples_individuals_info["sampleId2"]))
    #
    #     individual1_info = samples_individuals_info["individualInfo1"]
    #     individual2_info = samples_individuals_info["individualInfo2"]
    #     if individual1_info["individualId"] == "" or individual2_info["individualId"] == "":
    #         LOGGER.warning('No individual information available for sample {} and sample {}). Hence reported family relationship UNKNOWN'.format(
    #             samples_individuals_info["sampleId1"], samples_individuals_info["sampleId2"]))
    #         reported_relationship = "UNKNOWN"
    #     else:
    #         reported_relationship = []
    #         unknown_results = [False, False]
    #         if individual1_info["individualId"] in individual2_info["familyMembersRoles"].keys():
    #             reported_relationship.append(individual2_info["familyMembersRoles"][individual1_info["individualId"]])
    #         else:
    #             reported_relationship.append("UNKNOWN")
    #             unknown_results[0] = True
    #
    #         if individual2_info["individualId"] in individual1_info["familyMembersRoles"].keys():
    #             reported_relationship.append(
    #                 individual1_info["familyMembersRoles"][individual2_info["individualId"]])
    #         else:
    #             reported_relationship.append("UNKNOWN")
    #             unknown_results[1] = True
    #
    #         if all(unknown_results):
    #             reported_relationship = "SPOUSE, UNRELATED"
    #             LOGGER.info(
    #                 "UNRELATED family relationship found for sample {} (individual: {}) and sample {} (individual: {})".format(
    #                     samples_individuals_info["sampleId1"], individual1_info["individualId"], samples_individuals_info["sampleId2"],
    #                     individual2_info["individualId"]))
    #         elif any(unknown_results):
    #             LOGGER.warning(
    #                 'Family relationship discrepancy found for sample {} (individual: {}) and sample {} (individual: {}). Hence reported family relationship UNKNOWN'.format(
    #                     samples_individuals_info["sampleId1"], individual1_info["individualId"], samples_individuals_info["sampleId2"],individual2_info["individualId"]))
    #             reported_relationship = "UNKNOWN"
    #         else:
    #             reported_relationship = ', '.join(reported_relationship)
    #             LOGGER.info(
    #                 "Family relationship reported for sample {} (individual: {}) and sample {} (individual: {})".format(
    #                     samples_individuals_info["sampleId1"], individual1_info["individualId"], samples_individuals_info["sampleId2"],individual2_info["individualId"]))
    #
    #     return reported_relationship

    def parse_plink_genome(self, plink_genome_fpath):
        # Reading plink genome file (.genome)
        LOGGER.info('Getting PLINK results from file: "{}"'.format(plink_genome_fpath))
        input_genome_file_fhand = open(str(plink_genome_fpath))

        # Preparing relatedness results data model (scores)
        for index, line in enumerate(input_genome_file_fhand):
            genome_file_row_values = line.strip().split()
            if index != 0:
                # Getting values from PLINK .genome file block
                values = {"RT": str(genome_file_row_values[4]),
                          "ez": float(genome_file_row_values[5]),
                          "z0": float(genome_file_row_values[6]),
                          "z1": float(genome_file_row_values[7]),
                          "z2": float(genome_file_row_values[8]),
                          "PI_HAT": float(genome_file_row_values[9]),
                          "PHE": int(genome_file_row_values[10]),
                          "DST": float(genome_file_row_values[11]),
                          "PPC": float(genome_file_row_values[12]),
                          "RATIO": float(genome_file_row_values[13])
                        }
                score = common.Score(
                    sampleId1=str(genome_file_row_values[1]),
                    sampleId2=str(genome_file_row_values[3]),
                    reportedRelationship="",
                    inferredRelationship="",
                    validation="",
                    values=dict(values)
                )

                # Getting reported family relationship
                samples_info = self.executor["samples_info"]
                score.reportedRelationship = "SPOUSE, UNRELATED"
                if score.sampleId1 in samples_info:
                    if score.sampleId2 in samples_info[score.sampleId1].roles:
                        score.reportedRelationship = samples_info[score.sampleId1].roles[score.sampleId2]

                # all_samples_individuals_info = self.get_samples_individuals_info()
                # samples_individuals = {}
                # for sample,individual_info in all_samples_individuals_info.items():
                #     if sample == score["sampleId1"]:
                #         samples_individuals["sampleId1"] = sample
                #         samples_individuals["individualInfo1"] = individual_info
                #     if sample == score["sampleId2"]:
                #         samples_individuals["sampleId2"] = sample
                #         samples_individuals["individualInfo2"] = individual_info
                # score["reportedRelationship"] = RelatednessAnalysis.relatedness_report(samples_individuals)

                # if ((score.sampleId1 == self.executor["mother_id"] and score.sampleId2 == self.executor["father_id"])
                #     or (score.sampleId1 == self.executor["father_id"] and score.sampleId2 == self.executor["mother_id"])):
                #     score.reportedRelationship = "SPOUSE"
                # elif ((score.sampleId1 == self.executor["sample_id"] and score.sampleId2 == self.executor["mother_id"])
                #       or (score.sampleId1 == self.executor["mother_id"] and score.sampleId2 == self.executor["sample_id"])):
                #     if self.executor["sex"] == 1:
                #         score.reportedRelationship = "SON"
                #     elif self.executor["sex"] == 2:
                #         score.reportedRelationship = "DAUGTHER"
                #     else:
                #         score.reportedRelationship = "SON, DAUGTHER"
                # elif ((score.sampleId1 == self.executor["sample_id"] and score.sampleId2 == self.executor["father_id"])
                #       or (score.sampleId1 == self.executor["father_id"] and score.sampleId2 == self.executor["sample_id"])):
                #     if self.executor["sex"] == 1:
                #         score.reportedRelationship = "SON"
                #     elif self.executor["sex"] == 2:
                #         score.reportedRelationship = "DAUGTHER"
                #     else:
                #         score.reportedRelationship = "SON, DAUGTHER"

                # Inferring family relationship block:
                score.inferredRelationship = self.relatedness_inference(score.sampleId1, score.sampleId2, values)

                # Validating reported vs inferred family relationship results block:
                score.validation = RelatednessAnalysis.relatedness_validation(score.reportedRelationship, score.inferredRelationship)
                
                # Adding score to scores list:
                self.relatedness.scores.append(score)

    def run(self):
        """
        Execute the relatedness analysis
        """
        try:
            LOGGER.info('Starting %s', ANALYSIS_NAME)

            # Relatedness input files set up
            self.relatedness_setup()

            # Filtering VCF and renaming variants
            filtered_vcf_fpath = self.filter_variants()
            
            # Performing IBD analysis from PLINK
            plink_genome_fpath = self.relatedness_plink(filtered_vcf_fpath)

            # Getting and calculating relatedness scores: reported relationship, inferred relationship, validation
            self.parse_plink_genome(plink_genome_fpath)

            LOGGER.info('Complete successfully %s', ANALYSIS_NAME)
        except Exception as e:
            LOGGER.error("Error during %s: '%s'", ANALYSIS_NAME, format(e))
            raise