#!/usr/bin/env python3

import os
import logging
import gzip
import json

from utils import create_output_dir, execute_bash_command, generate_results_json
from family_qc.family_qc import FamilyQCExecutor
from family_qc.relatedness_results import RelatednessResults, Software, Scores, Values, Images, Attributes


LOGGER = logging.getLogger('variant_qc_logger')


class RelatednessAnalysis:
    def __init__(self, family_qc_executor_info):
        """
        """

        self.output_relatedness_dir = None
        self.pop_freq_file = None
        self.pop_exclude_var_file = None
        self.relatedness_thresholds_file = None
        self.family_qc_executor_info = family_qc_executor_info
        self.relatedness_results = RelatednessResults()


    def relatedness_setup(self):
        if isinstance(self.family_qc_executor_info, FamilyQCExecutor):
            self.set_relatedness_files()
            self.set_relatedness_dir()
        else:
            msg = "No instance of FamilyQCExecutor was found. Therefore relatedness analysis cannot be executed."
            LOGGER.error(msg)
            raise TypeError(msg)

    def set_relatedness_files(self):
        LOGGER.info('Checking and setting up relatedness files')
        if os.path.exists(os.path.join(self.family_qc_executor_info.resource_dir)):
            relatedness_files = {
                "pop_freq_file": os.path.join(self.family_qc_executor_info.resource_dir,'autosomes_1000G_QC_prune_in.frq'),
                "pop_exclude_var_file": os.path.join(self.family_qc_executor_info.resource_dir,'autosomes_1000G_QC.prune.out'),
                "relatedness_thresholds_file": os.path.join(self.family_qc_executor_info.resource_dir,'relatedness_thresholds.tsv')
                }
            for key,file in relatedness_files.items():
                if os.path.isfile(file):
                    if key == "pop_freq_file":
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
            msg = 'Directory "{}" does not exist'.format(os.path.join(self.family_qc_executor_info.resource_dir))
            LOGGER.error(msg)
            raise FileNotFoundError(msg)

    def set_relatedness_dir(self):
        output_relatedness_dir = create_output_dir(path_elements=[self.family_qc_executor_info.output_parent_dir, 'relatedness'])
        self.output_relatedness_dir = output_relatedness_dir

    def filter_rename_variants_vcf(self):
        # Reading VCF
        vcf_fhand = gzip.open(self.family_qc_executor_info.vcf_file, 'r')
        # Reading pop_freq file
        input_pop_freq_fhand = open(self.pop_freq_file, 'r')
        LOGGER.debug('Getting variant IDs to include in the VCF from file: "{}"'.format(self.pop_freq_file))
        variant_ids_to_include = [line.strip().split()[1] for line in input_pop_freq_fhand]

        # Create output dir and file
        filtered_vcf_outdir_fpath = create_output_dir(path_elements=[self.output_relatedness_dir, 'filtered_vcf'])
        output_file_name = 'filtered_vcf_' + os.path.basename(self.family_qc_executor_info.vcf_file)
        filtered_vcf_fpath = os.path.join(filtered_vcf_outdir_fpath, output_file_name)
        filtered_vcf_fhand = gzip.open(filtered_vcf_fpath, 'wt')
        LOGGER.debug('Generating filtered VCF with variant IDs under ID column: "{}"'.format(filtered_vcf_fpath))

        # Generate VCF with variant IDs under ID column
        for line in vcf_fhand:
            line = line.decode()
            if line.startswith('#'):
                # Writing VCF header as it is
                filtered_vcf_fhand.write(line)
                continue
            else:
                # Getting variant data
                variant_items = line.strip().split()
                variant_id = ':'.join([variant_items[0], variant_items[1], variant_items[3], variant_items[4]])
                if 'chr' not in variant_items[0]:
                    variant_id = 'chr' + variant_id
                if variant_id in variant_ids_to_include:
                    variant_items[2] = variant_id
                    filtered_vcf_fhand.write('\t'.join(variant_items) + '\n')
        LOGGER.info('Filtered VCF with variant IDs under ID column generated: "{}"'.format(filtered_vcf_fpath))

        # Return filtered VCF path (variant IDs under ID column)
        return filtered_vcf_fpath

    def get_samples_individuals_info(self):
        family_info_fhand = open(self.family_qc_executor_info.info_file)
        family_info_json = json.load(family_info_fhand)
        samples_individuals = {}
        for sample in self.family_qc_executor_info.sample_ids:
            samples_individuals[sample] = {'individualId': '', 'individualSex': 0, 'fatherId': 'NA', 'motherId': 'NA',
                                           'familyMembersRoles': 'NA'}

        LOGGER.debug('Getting individual information for each sample')
        for member in family_info_json['members']:
            for sample_member in member['samples']:
                if sample_member['id'] in self.family_qc_executor_info.sample_ids:
                    # Filling in individual info
                    LOGGER.debug('Individual information for sample "{}" found'.format(sample_member['id']))
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
        family_id = self.family_qc_executor_info.id_
        samples_individuals = self.get_samples_individuals_info()

        # Create dir for PLINK if it does not exist yet:
        plink_dir = create_output_dir(path_elements=[self.output_relatedness_dir, 'plink_IBD'])

        # Generating text file to update sex information
        sex_information_output_file_name = family_id + '_individual_sample_sex_information.txt'
        sex_information_output_fpath = os.path.join(plink_dir, sex_information_output_file_name)
        sex_information_output_fhand = open(sex_information_output_fpath, 'w')
        LOGGER.debug('Generating text file to update individual, sample, sex information: "{}"'.format(
            sex_information_output_fpath))

        # Generating text file to update parent-offspring relationships
        parent_offspring_output_file_name = family_id + '_parent_offspring_relationships.txt'
        parent_offspring_output_fpath = os.path.join(plink_dir, parent_offspring_output_file_name)
        parent_offspring_output_fhand = open(parent_offspring_output_fpath, 'w')
        LOGGER.debug('Generating text file to update parent-offspring relationships: "{}"'.format(parent_offspring_output_fpath))

        for sample in self.family_qc_executor_info.sample_ids:
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
        plink_dir = create_output_dir(path_elements=[self.output_relatedness_dir, 'plink_IBD'])
        sex_info_fpath, parent_offspring_fpath = self.generate_files_for_plink_fam_file()
        # Preparing PLINK commands
        plink_path = str(plink_path)
        files_prefix = self.family_qc_executor_info.id_ + "_plink_relatedness_results"
        plink_output_folder_files_prefix = os.path.join(plink_dir, files_prefix)
        plink_files_args = ["--vcf", str(filtered_vcf_fpath),
                      "--make-bed",
                      "--const-fid", self.family_qc_executor_info.id_,
                      "--chr", "1-22",
                      "--not-chr", "X,Y,MT",
                      "--allow-extra-chr",
                      "--snps-only",
                      "--biallelic-only", "strict",
                      "--vcf-half-call", "haploid",
                      "--update-sex", sex_info_fpath,
                      "--update-parents", parent_offspring_fpath,
                      "--out", plink_output_folder_files_prefix]
        cmd_plink_files = [plink_path] +  plink_files_args
        LOGGER.debug('Generating PLINK files')
        plink_files = execute_bash_command(cmd_plink_files)
        if plink_files[0] == 0:
            plink_files_generated = os.listdir(plink_dir)
            LOGGER.info('Files available in directory "{}":\n{}'.format(plink_dir, plink_files_generated))

            plink_ibd_args = ["--bfile", plink_output_folder_files_prefix,
                              "--genome", "rel-check",
                              "--read-freq", self.pop_freq_file,
                              "--exclude", self.pop_exclude_var_file,
                              "--out", plink_output_folder_files_prefix]
            cmd_plink_ibd = [plink_path] + plink_ibd_args
            LOGGER.debug("Performing IBD analysis")
            plink_ibd = execute_bash_command(cmd_plink_ibd)
            if plink_ibd[0] == 0:
                plink_files_generated = os.listdir(plink_dir)
                LOGGER.info('Files available in directory "{}":\n{}'.format(plink_dir, plink_files_generated))

            plink_genome_fpath = plink_output_folder_files_prefix + '.genome'
            if os.path.isfile(plink_genome_fpath) == False:
                LOGGER.error('File "{}" does not exist. Check:\nSTDOUT: "{}"\nSTDERR: "{}"'.format(plink_genome_fpath, plink_ibd[1], plink_ibd[2]))
                raise Exception(
                    'File "{}" does not exist. Check:\nSTDOUT: "{}"\nSTDERR: "{}"'.format(plink_genome_fpath, plink_ibd[1], plink_ibd[2]))
            else:
                # Filling in method, software, and attributes fields from the relatedness results data model'
                LOGGER.debug('Filling in method, software, and attributes fields from the relatedness results data model')
                self.relatedness_results.method = method
                all_parameters = plink_files_args + plink_ibd_args
                params = {}
                for index,parameter in enumerate(all_parameters):
                    if parameter.startswith('--'):
                        key = parameter
                        if (all_parameters[index+1].startswith('--'))==False:
                            value = all_parameters[index+1]
                        else:
                            value = ""
                        params[key] = value
                        self.relatedness_results.software = Software(name="plink", version="1.9", commit="", params=params)
                    else:
                        continue
                cli = ' '.join(cmd_plink_files + ['|'] + cmd_plink_ibd)
                files = []
                for dir in os.listdir(self.output_relatedness_dir):
                    files.extend(os.listdir(os.path.join(self.output_relatedness_dir,dir)))
                self.relatedness_results.attributes = Attributes(cli=cli, files=files)

                # Return path of .genome file generated by PLINK
                return plink_genome_fpath

    @staticmethod
    def relatedness_validation(reported_result, inferred_result):
        LOGGER.debug('Comparing reported {} and inferred {} results'.format(reported_result, inferred_result))
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
        LOGGER.debug('Getting relatedness thresholds from file: "{}"'.format(self.relatedness_thresholds_file))
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
        LOGGER.info("Inferring family relationship between sample '{}' and sample '{}' ".format(sampleId1, sampleId2))
        inference_groups = []
        for relationship, values in relationship_groups_thresholds_dict.items():
            # Check if PI_HAT, Z0, Z1, Z2 values (from PLINK .genome file) are within range (internal thresholds)
            if ((values['minPiHat']) <= score_values["PiHat"] <= values['maxPiHat']) and (
                    values['minZ0'] <= score_values["z0"] <= values['maxZ0']) and (
                    values['minZ1'] <= score_values["z1"] <= values['maxZ1']) and (
                    values['minZ2'] <= score_values["z2"] <= values['maxZ2']):
                inference_groups.append(str(relationship))
                continue
        if len(inference_groups) == 0:
            inferred_relationship = "UNKNOWN"
            LOGGER.info("UNKNOWN family relationship inferred between sample '{}' and sample '{}' ".format(sampleId1, sampleId2))
        else:
            inferred_relationship = ', '.join(inference_groups)
        LOGGER.info("Family relationship inferred between sample '{}' and sample '{}' ".format(sampleId1, sampleId2))
        
        return inferred_relationship

    @staticmethod
    def relatedness_report(samples_individuals_info):
        # Getting reported family relationship block:
        expected_keys = ["sampleId1","individualInfo1","sampleId2","individualInfo2"]
        if expected_keys != list(samples_individuals_info.keys()):
            msg = "Expected keys {} are not present in samples_individuals_info dictionary. Keys found: {}".format(expected_keys, samples_individuals_info.keys())
            LOGGER.error(msg)
            raise TypeError(msg)
        LOGGER.debug('Getting reported relatedness information for sample {} and sample {}'.format(samples_individuals_info["sampleId1"], samples_individuals_info["sampleId2"]))

        individual1_info = samples_individuals_info["individualInfo1"]
        individual2_info = samples_individuals_info["individualInfo2"]
        if individual1_info["individualId"] == "" or individual2_info["individualId"] == "":
            LOGGER.warning('No individual information available for sample {} and sample {}). Hence reported family relationship UNKNOWN'.format(
                samples_individuals_info["sampleId1"], samples_individuals_info["sampleId2"]))
            reported_relationship = "UNKNOWN"
        else:
            reported_relationship = []
            unknown_results = [False, False]
            if individual1_info["individualId"] in individual2_info["familyMembersRoles"].keys():
                reported_relationship.append(individual2_info["familyMembersRoles"][individual1_info["individualId"]])
            else:
                reported_relationship.append("UNKNOWN")
                unknown_results[0] = True

            if individual2_info["individualId"] in individual1_info["familyMembersRoles"].keys():
                reported_relationship.append(
                    individual1_info["familyMembersRoles"][individual2_info["individualId"]])
            else:
                reported_relationship.append("UNKNOWN")
                unknown_results[1] = True

            if all(unknown_results):
                reported_relationship = "SPOUSE, UNRELATED"
                LOGGER.info(
                    "UNRELATED family relationship found for sample {} (individual: {}) and sample {} (individual: {})".format(
                        samples_individuals_info["sampleId1"], individual1_info["individualId"], samples_individuals_info["sampleId2"],
                        individual2_info["individualId"]))
            elif any(unknown_results):
                LOGGER.warning(
                    'Family relationship discrepancy found for sample {} (individual: {}) and sample {} (individual: {}). Hence reported family relationship UNKNOWN'.format(
                        samples_individuals_info["sampleId1"], individual1_info["individualId"], samples_individuals_info["sampleId2"],individual2_info["individualId"]))
                reported_relationship = "UNKNOWN"
            else:
                reported_relationship = ', '.join(reported_relationship)
                LOGGER.info(
                    "Family relationship reported for sample {} (individual: {}) and sample {} (individual: {})".format(
                        samples_individuals_info["sampleId1"], individual1_info["individualId"], samples_individuals_info["sampleId2"],individual2_info["individualId"]))
        
        return reported_relationship

    def relatedness_scores(self, plink_genome_fpath):
        # Reading plink genome file (.genome)
        LOGGER.debug('Getting PLINK results from file: "{}"'.format(plink_genome_fpath))
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
                          "PiHat": float(genome_file_row_values[9])
                        }
                score = {"sampleId1": str(genome_file_row_values[1]),
                         "sampleId2": str(genome_file_row_values[3]),
                         "reportedRelationship": None,
                         "inferredRelationship": None,
                         "validation": None,
                         "values": values
                        }

                # Getting reported family relationship block:
                all_samples_individuals_info = self.get_samples_individuals_info()
                samples_individuals = {}
                for sample,individual_info in all_samples_individuals_info.items():
                    if sample == score["sampleId1"]:
                        samples_individuals["sampleId1"] = sample
                        samples_individuals["individualInfo1"] = individual_info
                    if sample == score["sampleId2"]:
                        samples_individuals["sampleId2"] = sample
                        samples_individuals["individualInfo2"] = individual_info
                score["reportedRelationship"] = RelatednessAnalysis.relatedness_report(samples_individuals)

                # Inferring family relationship block:
                score["inferredRelationship"] = self.relatedness_inference(score["sampleId1"],score["sampleId2"],values)

                # Validating reported vs inferred family relationship results block:
                score["validation"] = RelatednessAnalysis.relatedness_validation(score["reportedRelationship"], score["inferredRelationship"])
                
                # Adding score to scores list:
                self.relatedness_results.add_score(score)

    def relatedness(self):
        # Prepare reference file paths to use them later:
        resources_path = os.path.join(os.path.dirname(self.output_parent_dir),'resources')
        pop_freq_fpath = os.path.join(resources_path,'autosomes_1000G_QC_prune_in.frq')
        pop_exclude_var_fpath = os.path.join(resources_path,'autosomes_1000G_QC.prune.out')
        relatedness_thresholds_fpath = os.path.join(resources_path,'relatedness_thresholds.tsv')

        # Create output dir for relatedness analysis
        relatedness_output_dir_fpath = create_output_dir(path_elements=[self.output_parent_dir, 'relatedness'])

        # Filtering VCF and renaming variants
        filtered_vcf_fpath = self.filter_rename_variants_vcf(pop_freq_fpath, relatedness_output_dir_fpath)
        # Performing IBD analysis from PLINK
        relatedness_results, plink_genome_fpath = self.relatedness_plink(filtered_vcf_fpath, pop_freq_fpath, pop_exclude_var_fpath, relatedness_output_dir_fpath)
        # Inferring family relationships
        relatedness_results = self.relatedness_inference(relatedness_thresholds_fpath, plink_genome_fpath, relatedness_results)
        # Getting reported family relationships and validating inferred vs reported results
        relatedness_results = self.relatedness_report(relatedness_results)

        # Generating file with results
        relatedness_results_fpath = self.generate_relatedness_results_file(relatedness_results, relatedness_output_dir_fpath)

    def run(self):
        # Checking data
        # self.checking_data()  # TODO check input data (config parameters)

        # Running family QC steps
        # Run relatedness analysis
        self.relatedness()