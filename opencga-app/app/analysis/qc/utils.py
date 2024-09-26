#!/usr/bin/env python3

import os
import logging
import subprocess
import json

LOGGER = logging.getLogger('variant_qc_logger')


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
    LOGGER.debug('Executing in bash: "{}"'.format(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    stdout, stderr = p.communicate()
    p.wait()
    return_code = p.returncode

    if return_code != 0:
        msg = 'Command line "{}" returned non-zero exit status "{}"\nSTDOUT: "{}"\nSTDERR: "{}"'.format(
            cmd, return_code, stdout, stderr
        )
        LOGGER.error(msg)
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
