#!/usr/bin/env python3

import os
import logging
import gzip
import json

from utils import create_output_dir, execute_bash_command

LOGGER = logging.getLogger('variant_qc_logger')


class FamilyQCExecutor:
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

    def filter_rename_variants_vcf(self, pop_freq_fpath, outdir_fpath):
        # Reading VCF
        vcf_fhand = gzip.open(self.vcf_file, 'r')
        # Reading pop_freq file
        input_pop_freq_fhand = open(str(pop_freq_fpath), 'r')
        LOGGER.debug('Getting variant IDs to include in the VCF from file: "{}"'.format(pop_freq_fpath))
        variant_ids_to_include = [line.strip().split()[1] for line in input_pop_freq_fhand]

        # Create output dir and file
        filtered_vcf_outdir_fpath = create_output_dir(path_elements=[str(outdir_fpath), 'filtered_vcf'])
        output_file_name = 'filtered_vcf_' + str(self.vcf_file.split('/')[-1])
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
        family_info_fhand = open(self.info_file)
        family_info_json = json.load(family_info_fhand)
        samples_individuals = {}
        for sample in self.sample_ids:
            samples_individuals[sample] = {'individualId': '', 'individualSex': 0, 'fatherId': 'NA', 'motherId': 'NA',
                                           'familyMembersRoles': 'NA'}

        LOGGER.debug('Getting individual information for each sample')
        for member in family_info_json['members']:
            for sample_member in member['samples']:
                if sample_member['id'] in self.sample_ids:
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

    def generate_files_for_plink_fam_file(self, outdir_fpath):
        # Getting family id and sample_individuals_info
        family_id = self.id_
        samples_individuals = self.get_samples_individuals_info()

        # Generating text file to update sex information
        sex_information_output_file_name = family_id + '_individual_sample_sex_information.txt'
        sex_information_output_fpath = os.path.join(str(outdir_fpath), sex_information_output_file_name)
        sex_information_output_fhand = open(sex_information_output_fpath, 'w')
        LOGGER.debug('Generating text file to update individual, sample, sex information: "{}"'.format(
            sex_information_output_fpath))

        # Generating text file to update parent-offspring relationships
        parent_offspring_output_file_name = family_id + '_parent_offspring_relationships.txt'
        parent_offspring_output_fpath = os.path.join(str(outdir_fpath), parent_offspring_output_file_name)
        parent_offspring_output_fhand = open(parent_offspring_output_fpath, 'w')
        LOGGER.debug(
            'Generating text file to update parent-offspring relationships: "{}"'.format(parent_offspring_output_fpath))

        for sample in self.sample_ids:
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

    def relatedness_results_data_model(self):
        relatedness_json = {
            "method": "",
            "software": {
                "name": "",
                "version": "",
                "commit": "",
                "params": {}
            },
            "scores": [
                {
                    "sampleId1": "",
                    "sampleId2": "",
                    "reportedRelationship": "",
                    "inferredRelationship": "",
                    "validation": "",
                    "values": {
                        "RT": "",
                        "ez": "",
                        "z0": "",
                        "z1": "",
                        "z2": "",
                        "PiHat": ""
                    }
                }
            ],
            "images": [
                {
                    "name": "",
                    "base64": "",
                    "description": ""
                }
            ],
            "attributes": {
                "cli": "",
                "files": [],
                "JOB_ID": ""
            }
        }
        return relatedness_json

    def relatedness_plink(self, filtered_vcf_fpath, pop_freq_fpath, pop_exclude_var_fpath, outdir_fpath, plink_path,
                          method="PLINK/IBD"):
        LOGGER.info('Method: {}'.format(method))
        plink_outdir_fpath = create_output_dir(path_elements=[str(outdir_fpath), 'plink_IBD'])
        sex_info_fpath, parent_offspring_fpath = self.generate_files_for_plink_fam_file(outdir_fpath=plink_outdir_fpath)
        # Preparing PLINK commands
        plink_path = str(plink_path)
        files_prefix = self.id_ + "_plink_relatedness_results"
        plink_output_folder_files_prefix = os.path.join(plink_outdir_fpath, files_prefix)
        plink_files_args = ["--vcf", str(filtered_vcf_fpath),
                      "--make-bed",
                      "--const-fid", self.id_,
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
            plink_files_generated = os.listdir(plink_outdir_fpath)
            LOGGER.info('Files available in directory "{}":\n{}'.format(plink_outdir_fpath, plink_files_generated))

            plink_ibd_args = ["--bfile", plink_output_folder_files_prefix,
                              "--genome", "rel-check",
                              "--read-freq", str(pop_freq_fpath),
                              "--exclude", str(pop_exclude_var_fpath),
                              "--out", plink_output_folder_files_prefix]
            cmd_plink_ibd = [plink_path] + plink_ibd_args
            LOGGER.debug("Performing IBD analysis")
            plink_ibd = execute_bash_command(cmd_plink_ibd)
            if plink_ibd[0] == 0:
                plink_files_generated = os.listdir(plink_outdir_fpath)
                LOGGER.info('Files available in directory "{}":\n{}'.format(plink_outdir_fpath, plink_files_generated))

            plink_genome_fpath = plink_output_folder_files_prefix + '.genome'
            if os.path.isfile(plink_genome_fpath) == False:
                LOGGER.error('File "{}" does not exist. Check:\nSTDOUT: "{}"\nSTDERR: "{}"'.format(plink_genome_fpath,
                                                                                                   plink_ibd[1],
                                                                                                   plink_ibd[2]))
                raise Exception(
                    'File "{}" does not exist. Check:\nSTDOUT: "{}"\nSTDERR: "{}"'.format(plink_genome_fpath,
                                                                                          plink_ibd[1], plink_ibd[2]))
            else:
                # Preparing relatedness results data model (method, software, and attributes)
                relatedness_results = self.relatedness_results_data_model()
                relatedness_results["method"] = method
                relatedness_results["software"]["name"] = "plink"
                relatedness_results["software"]["version"] = "1.9"
                all_parameters = plink_files_args + plink_ibd_args
                for index,parameter in enumerate(all_parameters):
                    if parameter.startswith('--'):
                        key = parameter
                        if (all_parameters[index+1].startswith('--'))==False:
                            value = all_parameters[index+1]
                        else:
                            value = ""
                        relatedness_results["software"]["params"][key] = value
                    else:
                        continue
                cli = cmd_plink_files + ['|'] + cmd_plink_ibd
                relatedness_results["attributes"]["cli"] = ' '.join(cli)
                files = []
                for dir in os.listdir(str(outdir_fpath)):
                    files.extend(os.listdir(os.path.join(str(outdir_fpath),dir)))
                relatedness_results["attributes"]["files"] = files
                # Return relatedness results json with method information and path of .genome file generated by PLINK
                return [relatedness_results, plink_genome_fpath]

    @staticmethod
    def relatedness_validation(reported_result, inferred_result):
        LOGGER.debug('Comparing reported {} and inferred {} results'.format(reported_result, inferred_result))
        if 'UNKNOWN' == reported_result or reported_result == "":
            validation = "UNKNOWN"
        else:
            reported_result = set(reported_result.split(', '))
            inferred_result = set(inferred_result.split(', '))
            if len(reported_result) != len(inferred_result):
                validation = "FAIL"
            else:
                if reported_result == inferred_result:
                    validation = "PASS"
                else:
                    validation = "FAIL"
        # Return validation result
        return validation

    def relatedness_inference(self, relatedness_thresholds_fpath, plink_genome_fpath, relatedness_results):
        # Reading relatedness thresholds file (.tsv)
        LOGGER.debug('Getting relatedness thresholds from file: "{}"'.format(relatedness_thresholds_fpath))
        relatedness_thresholds_fhand = open(str(relatedness_thresholds_fpath))
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

        # Reading plink genome file (.genome)
        LOGGER.debug('Getting PLINK results from file: "{}"'.format(plink_genome_fpath))
        input_genome_file_fhand = open(str(plink_genome_fpath))

        # Preparing relatedness results data model (scores)
        relatedness_scores = []
        for index, line in enumerate(input_genome_file_fhand):
            genome_file_row_values = line.strip().split()
            if index != 0:
                # Getting values from PLINK .genome file block
                score = self.relatedness_results_data_model()["scores"][0]
                score["sampleId1"] = str(genome_file_row_values[1])
                score["sampleId2"] = str(genome_file_row_values[3])
                score["values"]["RT"] = str(genome_file_row_values[4])
                score["values"]["ez"] = str(genome_file_row_values[5])
                score["values"]["z0"] = str(genome_file_row_values[6])
                score["values"]["z1"] = str(genome_file_row_values[7])
                score["values"]["z2"] = str(genome_file_row_values[8])
                score["values"]["PiHat"] = str(genome_file_row_values[9])

                # Inferring family relationship block:
                LOGGER.debug("Inferring family relationship between sample {} and sample {} ".format(str(genome_file_row_values[1]),str(genome_file_row_values[3])))
                inference_groups = []
                for relationship, values in relationship_groups_thresholds_dict.items():
                    # Check if PI_HAT, Z0, Z1, Z2 values (from PLINK .genome file) are within range (internal thresholds)
                    if ((values['minPiHat']) <= float(score["values"]["PiHat"]) <= values['maxPiHat']) and (
                            values['minZ0'] <= float(score["values"]["z0"]) <= values['maxZ0']) and (
                            values['minZ1'] <= float(score["values"]["z1"]) <= values['maxZ1']) and (
                            values['minZ2'] <= float(score["values"]["z2"]) <= values['maxZ2']):
                        inference_groups.append(str(relationship))
                        continue
                if len(inference_groups) == 0:
                    score["inferredRelationship"] = "UNKNOWN"
                    LOGGER.info("UNKNOWN family relationship inferred between sample {} and sample {} ".format(
                        str(genome_file_row_values[1]), str(genome_file_row_values[3])))
                else:
                    score["inferredRelationship"] = ', '.join(inference_groups)
                LOGGER.info("Family relationship inferred between sample {} and sample {} ".format(str(genome_file_row_values[1]),str(genome_file_row_values[3])))
                relatedness_scores.append(score)
        relatedness_results["scores"] = relatedness_scores

        # Return dict/json with plink and inferred results
        return relatedness_results

    def relatedness_report(self, relatedness_inference_results):
        samples_individuals = self.get_samples_individuals_info()
        # Getting reported family relationship block:
        relatedness_results = relatedness_inference_results
        for score_result in relatedness_inference_results["scores"]:
            LOGGER.debug(
                'Getting reported relatedness information for sample {} and sample {}'.format(score_result["sampleId1"],
                                                                                              score_result[
                                                                                                  "sampleId2"]))
            reported_relationship = []
            individual1_info = samples_individuals[score_result["sampleId1"]]
            individual2_info = samples_individuals[score_result["sampleId2"]]
            if individual1_info["individualId"] == "" or individual2_info["individualId"] == "":
                LOGGER.warning(
                    'No individual information available for sample {} and sample {}). Hence reported family relationship UNKNOWN'.format(
                        score_result["sampleId1"], score_result["sampleId2"]))
                relatedness_results["scores"]["reportedRelationship"] = "UNKNOWN"
                continue
            else:
                unknown_results = [False, False]
                if individual1_info["individualId"] in individual2_info["familyMembersRoles"].keys():
                    reported_relationship.append(
                        individual2_info["familyMembersRoles"][individual1_info["individualId"]])
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
                    score_result["reportedRelationship"] = "SPOUSE, UNRELATED"
                    LOGGER.info(
                        "UNRELATED family relationship found for sample {} (individual: {}) and sample {} (individual: {})".format(
                            score_result["sampleId1"], individual1_info["individualId"], score_result["sampleId2"],
                            individual2_info["individualId"]))
                elif any(unknown_results):
                    LOGGER.warning(
                        'Family relationship discrepancy found for sample {} (individual: {}) and sample {} (individual: {}). Hence reported family relationship UNKNOWN'.format(
                            score_result["sampleId1"], individual1_info["individualId"], score_result["sampleId2"],
                            individual2_info["individualId"]))
                    score_result["reportedRelationship"] = "UNKNOWN"
                else:
                    score_result["reportedRelationship"] = ', '.join(reported_relationship)
                    LOGGER.info(
                        "Family relationship reported for sample {} (individual: {}) and sample {} (individual: {})".format(
                            score_result["sampleId1"], individual1_info["individualId"], score_result["sampleId2"],
                            individual2_info["individualId"]))

            # Validating reported vs inferred family relationship results block:
            validation_result = FamilyQCExecutor.relatedness_validation(score_result["reportedRelationship"],
                                                            score_result["inferredRelationship"])
            score_result["validation"] = validation_result

        # Return dict/json with plink, inferred, reported and validation results
        return relatedness_results

    def relatedness_results_json(self, relatedness_results, outdir_fpath):
        relatedness_output_dir_fpath = outdir_fpath

        # Generating json file with relatedness results
        relatedness_results_file_name = 'relatedness.json'
        relatedness_results_fpath = os.path.join(relatedness_output_dir_fpath, relatedness_results_file_name)
        LOGGER.debug('Generating json file with relatedness results. File path: "{}"'.format(relatedness_results_fpath))
        json.dump(relatedness_results, open(relatedness_results_fpath, 'w'))
        LOGGER.info('Json file with relatedness results generated. File path: "{}"'.format(relatedness_results_fpath))

        # Return json file path with relatedness results
        return relatedness_results_fpath

    def relatedness(self):
        # Set up. Prepare reference file paths to use them later:
        pop_freq_fpath = "/path/to/pop_freq_prune_in.frq"
        pop_exclude_var_fpath = "/path/to/pop_exclude_var.prune.out"
        relatedness_thresholds_fpath = "/path/to/relatedness_thresholds.tsv"

        # Create output dir for relatedness analysis
        relatedness_output_dir_fpath = create_output_dir(path_elements=[self.output_parent_dir, 'relatedness'])

        # Filtering VCF and renaming variants
        filtered_vcf_fpath = self.filter_rename_variants_vcf(pop_freq_fpath, relatedness_output_dir_fpath)
        # Performing IBD analysis from PLINK
        method, plink_genome_fpath = self.relatedness_plink(filtered_vcf_fpath, pop_freq_fpath, pop_exclude_var_fpath,
                                                            relatedness_output_dir_fpath)
        # Inferring family relationships
        relatedness_inference_dict = self.relatedness_inference(relatedness_thresholds_fpath, method,
                                                                plink_genome_fpath)
        # Getting reported family relationships and validating inferred vs reported results
        relatedness_results_dict = self.relatedness_report(relatedness_inference_dict)
        # Generating file with results
        relatedness_results_json_fpath = self.relatedness_results_json(relatedness_results_dict,
                                                                       relatedness_output_dir_fpath)

    def run(self):
        # Checking data
        # self.checking_data()  # TODO check input data (config parameters)

        # Running family QC steps
        # Run relatedness analysis
        self.relatedness()





