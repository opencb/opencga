#!/usr/bin/env python3

import os
import logging
import subprocess

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
