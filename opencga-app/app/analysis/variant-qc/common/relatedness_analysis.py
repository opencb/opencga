#!/usr/bin/env python3

import logging
import os
import pysam

from common.relatedness import Relatedness, Score
from quality_control import Software
from utils import (RELATEDNESS_FREQS_FILE, RELATEDNESS_THRESHOLDS_FILE, RELATEDNESS_PRUNE_IN_MARKERS_FILE,
                   create_output_dir, execute_bash_command, create_sex_file, create_parents_file, create_phenotype_file,
                   get_family_id, get_contig_prefix)

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

        self.prune_in_markers_fpath = None
        self.freqs_fpath = None
        self.thresholds_fpath = None
        self.executor = executor
        self.relatedness = Relatedness()

    def relatedness_setup(self):
        if self.executor != None:
            self.set_relatedness_files()
        else:
            msg = "No instance of FamilyQCExecutor was found. Therefore relatedness analysis cannot be executed."
            LOGGER.error(msg)
            raise TypeError(msg)

    def set_relatedness_files(self):
        LOGGER.info('Checking and setting up relatedness files')
        resources_fpath = self.executor["resource_dir"]
        if os.path.exists(resources_fpath):
            relatedness_files = {
                "prune_in_markers_file": os.path.join(resources_fpath, RELATEDNESS_PRUNE_IN_MARKERS_FILE),
                "freqs_file": os.path.join(resources_fpath, RELATEDNESS_FREQS_FILE),
                "thresholds_file": os.path.join(resources_fpath, RELATEDNESS_THRESHOLDS_FILE)
                }
            for key,file in relatedness_files.items():
                if os.path.isfile(file):
                    if key == "prune_in_markers_file":
                        self.prune_in_markers_fpath = file
                    elif key == "freqs_file":
                        self.freqs_fpath = file
                    else:
                        self.thresholds_fpath = file
                    LOGGER.info('File {} set up successfully'.format(file))
                else:
                    msg = 'File "{}" does not exist'.format(file)
                    LOGGER.error(msg)
                    raise FileNotFoundError(msg)
        else:
            msg = 'Directory "{}" does not exist'.format(resources_fpath)
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
        LOGGER.info(cmd)
        cmd_result = execute_bash_command(cmd)
        pass_vars = int(cmd_result[1].strip())
        LOGGER.info(f"Number of PASS variants: {pass_vars}")


        # Check if input VCF uses 'chr' prefix in contig names:
        prefix = get_contig_prefix(vcf_file)
        LOGGER.info(f"Contig prefix '{prefix}' in input VCF: {vcf_file}")
        if prefix == '':
            prefix = "chr"
            LOGGER.info(f"Setting contig prefix to '{prefix}'")

        # Annotate input VCF with chr coordinate IDs, filter for chr X pruned variants and, if annotated, for PASS variants:
        if pass_vars == 0:
            LOGGER.debug("Annotating and filtering '{}' for all pruned chr X variants".format(vcf_file))
            LOGGER.info("WARNING: no FILTER information available, input data will not be filtered for PASS variants, results may be unreliable")
            cmd = f"bcftools annotate --set-id '{prefix}%CHROM\:%POS\:%REF\:%FIRST_ALT' {vcf_file} -Oz | bcftools view --include ID==@{self.prune_in_markers_fpath} -Oz -o {filtered_vcf_path}"
            LOGGER.info(cmd)
            execute_bash_command(cmd)
        else:
            LOGGER.debug("Annotating and filtering '{}' for chr X pruned PASS variants".format(vcf_file))
            cmd = f"bcftools annotate --set-id '{prefix}%CHROM\:%POS\:%REF\:%FIRST_ALT' {vcf_file} -Oz | bcftools view --include ID==@{self.prune_in_markers_fpath} -Oz | bcftools view -i 'FILTER=\"PASS\"' -Oz -o {filtered_vcf_path}"
            LOGGER.info(cmd)
            execute_bash_command(cmd)

        # Return filtered VCF path:
        return filtered_vcf_path

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

        cmd = f"{plink_path} --vcf {filtered_vcf_fpath} --make-bed --const-fid {family_id} --allow-extra-chr --snps-only --biallelic-only strict --vcf-half-call haploid --update-sex {sex_fpath} --update-parents {parents_fpath} --pheno {phenotype_fpath} --out {plink_prefix}"
        LOGGER.info('Generating PLINK files (--make-bed)')
        execute_bash_command(cmd)
        files_generated = os.listdir(plink_dir)
        LOGGER.info('Files available in directory "{}":\n{}'.format(plink_dir, files_generated))

        cmd = f"{plink_path} --bfile {plink_prefix} --genome rel-check --read-freq {self.freqs_fpath} --out {plink_prefix}"
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
        LOGGER.info('Getting relatedness thresholds from file: "{}"'.format(self.thresholds_fpath))
        relatedness_thresholds_fhand = open(self.thresholds_fpath)
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
                          "RATIO": str(genome_file_row_values[13])
                        }
                score = Score(
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