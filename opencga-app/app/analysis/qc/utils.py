#!/usr/bin/env python3

import base64
import json
import logging
import os
import subprocess
from pydantic import BaseModel, Field
from typing import Dict, List

LOGGER = logging.getLogger('variant_qc_logger')

# Relatedness resources
RELATEDNESS_PRUNE_IN_MARKERS_FILE = "relatedness_prune_in_markers.txt"
RELATEDNESS_FREQS_FILE = "relatedness_freqs.txt"
RELATEDNESS_THRESHOLDS_FILE =  "relatedness_thresholds.tsv"

# Inferred-sex resources
INFERRED_SEX_CHRX_PRUNE_IN_MARKERS_FILE = "inferred_sex_chrX_prune_in_markers.txt"
INFERRED_SEX_CHRX_FREQS_FILE = "inferred_sex_chrX_freqs.txt"
INFERRED_SEX_KARYOTYPIC_THRESHOLDS_FILE = "inferred_sex_karyotypic_thresholds.json"
INFERRED_SEX_REFERENCE_VALUES_FILE = "inferred_sex_reference_values.txt"

class SampleInfo(BaseModel):
    sampleId: str
    fatherSampleId: str
    motherSampleId: str
    individualId: str
    familyIds: List[str] = Field(default_factory=list)
    roles: Dict[str, str] = Field(default_factory=dict)
    sex: int
    phenotype: int

def get_individual_sex(individual):
    sex = 0
    if individual.get("sex") and individual.get("sex")["id"]:
        if "MALE" == individual.get("sex")["id"]:
            sex = 1
        elif "FEMALE" == individual.get("sex")["id"]:
            sex = 2
    return sex

def get_individual_phenotype(individual):
    if individual.get("disorders") and len(individual.get("disorders")) > 0:
        # Affected
        return 1
    else:
        # Unaffected
        return 0

def get_sex_from_individual_id(individual_id, samples_info):
    for sample_info in samples_info.values():
        if sample_info.individualId == individual_id:
            return sample_info.sex
    return None

def get_samples_info_from_individual(individual, sample_ids):
    samples_info = {}

    if individual.get("samples"):
        for sample in individual.get("samples"):
            sample_id = sample["id"]
            if  sample_id in sample_ids:
                info = SampleInfo(sampleId=sample_id,
                                  fatherSampleId="0",
                                  motherSampleId="0",
                                  individualId=individual.get("id"),
                                  familyIds=individual["familyIds"],
                                  roles={},
                                  sex=get_individual_sex(individual),
                                  phenotype=get_individual_phenotype(individual))

                samples_info[sample_id] = info

    return samples_info

def get_sample_info_from_individual_id(individual_id, samples_info):
    for sample_id, sample_info in samples_info.items():
        if sample_info.individualId == individual_id:
            return sample_info
    return None

def get_sample_id_from_individual_id(individual_id, samples_info):
    for sample_id, sample_info in samples_info.items():
        if sample_info.individualId == individual_id:
            return sample_id
    return None

def contains_trio(samples_info):
    for sample_info in samples_info.values():
        if sample_info.fatherSampleId != 0 and sample_info.motherSampleId != 0:
            return True
    return False

def get_family_id(samples_info):
    for sample_info in samples_info.values():
        if len(sample_info.familyIds) > 0:
            return sample_info.familyIds[0]
    return None

def create_sex_file(path, samples_info):
    # Sex file format: FamilyID SampleID Sex (0 = UNKNOWN, 1 = MALE, 2 = FEMALE)
    sex_fpath = os.path.join(path, "sex.txt")
    LOGGER.info(f"Generating text file to update sex information: '{sex_fpath}'")
    with open(sex_fpath, 'w') as file:
        for sample_info in samples_info.values():
            file.write('\t'.join([sample_info.familyIds[0], sample_info.sampleId, str(sample_info.sex)]) + '\n')
    return sex_fpath

def create_parents_file(path, samples_info):
    # Parents file format: FamilyID SampleID FatherID MotherID
    parents_fpath = os.path.join(path, "parents.txt")
    LOGGER.info(f"Generating text file to update parents information: '{parents_fpath}'")
    with open(parents_fpath, 'w') as file:
        for sample_info in samples_info.values():
            file.write('\t'.join([sample_info.familyIds[0], sample_info.sampleId, sample_info.fatherSampleId, sample_info.motherSampleId]) + '\n')
    return parents_fpath

def create_phenotype_file(path, samples_info):
    # Phenotype file format: FamilyID SampleID Phenotype (1 = AFFECTED, 2 = UNAFFECTED, -9 =  MISSING)
    phenotype_fpath = os.path.join(path, "phenotype.txt")
    LOGGER.info(f"Generating text file to update phenotype information: '{phenotype_fpath}'")
    with open(phenotype_fpath, 'w') as file:
        for sample_info in samples_info.values():
            file.write('\t'.join([sample_info.familyIds[0], sample_info.sampleId, str(sample_info.phenotype)]) + '\n')
    return phenotype_fpath

def create_output_dir(path_elements):
    """Create output dir

    :param list path_elements: List of the elements that compose the path
    :returns: The created output dir path
    """
    outdir_fpath = os.path.join(*path_elements)
    LOGGER.debug('Creating output directory: "{}"'.format(outdir_fpath))
    os.makedirs(outdir_fpath, exist_ok=True)  # Creating output dir if it does not exist

    return outdir_fpath

def execute_bash_command(cmd):
    """Run a bash command

    :param str cmd: Command line
    :returns: Return code, stdout, stderr
    """
    LOGGER.info(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    stdout, stderr = p.communicate()
    p.wait()
    return_code = p.returncode

    if return_code != 0:
        msg = f"Error code {return_code} when executing command {cmd}"
        LOGGER.error(msg)
        if stdout != None:
            LOGGER.error(stdout.decode())
        if stderr != None:
            LOGGER.error(stderr.decode())
        raise Exception(msg)

    return return_code, stdout, stderr

def generate_results_json(results: dict, outdir_path: str):
    """
    Generate a JSON file.
    
    :param results: dict object
    :param outdir_path: Path to the output directory where the JSON file will be stored
    """
    results_file_name = 'results.json'
    results_fpath = os.path.join(outdir_path, results_file_name)
    LOGGER.debug('Generating JSON file with results. File path: "{}"'.format(results_fpath))

    with open(results_fpath, 'w') as file:
        json.dump(results, file, indent=2)
        LOGGER.info('Finished writing json file with results: "{}"'.format(results_fpath))

def get_base64_image(image_fpath: str):
        # Read the image file as binary data
        with open(image_fpath, "rb") as image_file:
            binary_data = image_file.read()

        # Encode the binary data as a Base64 string
        return base64.b64encode(binary_data).decode('utf-8')
