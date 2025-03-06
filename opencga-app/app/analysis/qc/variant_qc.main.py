#!/usr/bin/env python3

import os
import sys
import argparse
import logging
import json
import shutil
import gzip
from datetime import datetime

from utils import create_output_dir
#from sample_qc.sample_qc import SampleQCExecutor
from individual_qc.individual_qc import IndividualQCExecutor
from family_qc.family_qc import FamilyQCExecutor


VERSION = '0.0.1'
LOGGER = logging.getLogger('variant_qc_logger')


def get_parser():
    """Parse input arguments

    :return: The argument parser
    """

    parser = argparse.ArgumentParser(description='This program runs variant QC on sample/individual/family')

    parser.add_argument('-i', '--vcf-file', dest='vcf_file', required=True,
                        help='comma-separated VCF file paths')
    parser.add_argument('-j', '--info-json', dest='info_json', required=True,
                        help='comma-separated info JSON file paths')
    parser.add_argument('-b', '--bam-file', dest='bam_file',
                        help='comma-separated BAM file paths')
    parser.add_argument('-q', '--qc-type', dest='qc_type', choices = ['sample', 'individual', 'family'], required=True,
                        help='type of QC')
    parser.add_argument('-c', '--config', dest='config', required=True,
                        help='configuration file path')
#     parser.add_argument('-r', '--resource-dir', dest='resource_dir', default='resources',
#                         help='resources directory path')
    parser.add_argument('-o', '--output-dir', dest='output_dir',
                        help='output directory path')

    parser.add_argument('-l', '--log-level', dest='log_level',
                        default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='provide logging level')
    parser.add_argument('-v', '--version', dest='version', action='version', version=VERSION,
                        help='outputs the program version')
    return parser


def check_args(args):
    """Check input arguments

    :param Namespace args: Input arguments
    """

    # Checking we have the same number of VCF files and info JSON files
    num_vcf_files = len(args.vcf_file.split(','))
    num_info_jsons = len(args.info_json.split(','))
    if num_vcf_files != num_info_jsons:
        msg = 'Different number of VCF files ({}) and info JSON files ({})'.format(num_vcf_files, num_info_jsons)
        LOGGER.error(msg)
        raise ValueError(msg)

    # Checking we have the same number of VCF files and BAM files
    if args.bam_file:
        num_bam_files = len(args.bam_file.split(','))
        if num_vcf_files != num_bam_files:
            msg = 'Different number of VCF files ({}) and BAM files ({})'.format(num_vcf_files, num_bam_files)
            LOGGER.error(msg)
            raise ValueError(msg)


def check_input(vcf_fpath, info_fpath, qc_type):
    """Check that there is only one ID (sample, ind or family) and whether VCF file and info json have the same samples

    :param str vcf_fpath: VCF input file path
    :param str info_fpath: Info JSON input file path
    :param str qc_type: QC type ('sample', 'individual' or 'family')
    :return: The sample IDs from the VCF file and the ID in the info JSON file
    """

    # Getting sample IDs from input VCF file
    LOGGER.debug('Getting sample IDs from input VCF file "{}"'.format(vcf_fpath))
    vcf_fhand = gzip.open(vcf_fpath, 'r')
    vcf_sample_ids = []
    for line in vcf_fhand:
        line = line.decode()
        if line.startswith('#CHROM'):
            vcf_sample_ids = line.strip().split()[9:]
            break
    vcf_sample_ids.sort()
    vcf_fhand.close()

    # Reading info JSON file
    LOGGER.debug('Getting sample IDs and {} ID from info JSON file "{}"'.format(qc_type, info_fpath))
    info_fhand = open(info_fpath, 'r')
    info_json = json.load(info_fhand)
    info_fhand.close()

    # Getting IDs and sample IDs from info JSON file
    info_ids = []
    info_sample_ids = []
    record = info_json
    # Getting all IDs in info file
    info_ids.append(record['id'])
    # Family
    if qc_type == 'family' and 'members' in record:
        info_sample_ids = [sample['id']
                            for member in record['members'] if 'samples' in member
                            for sample in member['samples'] if 'id' in sample]
    # Individual
    elif qc_type == 'individual' and 'samples' in record:
        info_sample_ids = [sample['id'] for sample in record['samples'] if 'id' in sample]
    # Sample
    elif qc_type == 'sample' and 'id' in record:
        info_sample_ids.append(record['id'])
    info_sample_ids.sort()

    # Checking whether input file and info file have the same samples
    LOGGER.debug('Sample IDs found in VCF file: "{}"'.format(vcf_sample_ids))
    LOGGER.debug('Sample IDs found in JSON file: "{}"'.format(info_sample_ids))
    if not vcf_sample_ids == info_sample_ids:
        msg = 'Different samples in VCF file ("{}") and info JSON file ("{}")'.format(vcf_sample_ids, info_sample_ids)
        LOGGER.error(msg)
        raise ValueError(msg)

    # Checking that there is only one ID
    LOGGER.debug('{} ID found in info JSON file: "{}"'.format(qc_type.capitalize(), info_sample_ids))
    if not len(info_ids) == 1:
        msg = 'More than one ID found in "{}": "{}"'.format(info_fpath, info_ids)
        LOGGER.error(msg)
        raise ValueError(msg)

    return vcf_sample_ids, info_ids[0]

def get_qc_executor(qc_type):
    """Get QC executor

    :param str qc_type: QC type ('sample', 'individual' or 'family')
    :return: The QC class executor corresponding to the specified type
    """

    qc_executor = None
    if qc_type == 'sample':
        qc_executor = SampleQCExecutor
    elif qc_type == 'individual':
        qc_executor = IndividualQCExecutor
    elif qc_type == 'family':
        qc_executor = FamilyQCExecutor
    return qc_executor

def create_logger(level, output_dir):
    """Create logging system

    :param str level: Logging verbosity level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    :param str output_dir: Output directory
    :return: The logger
    """

    # Create logger
    logger = logging.getLogger('variant_qc_logger')
    logger.setLevel(level)

    # Create file handler and set level
    fname = 'variant_qc_' + str(datetime.now().strftime('%Y%m%d%H%M%S')) + '.log'
    fh = logging.FileHandler(os.path.join(output_dir, fname))
    fh.setLevel(logging.DEBUG)  # DEBUG as fixed level for written logs

    # Create console handler and set level
    ch = logging.StreamHandler()
    ch.setLevel(level)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s', '%Y/%m/%d %H:%M:%S')
    fh.setFormatter(formatter)  # Add formatter to file handler
    ch.setFormatter(formatter)  # Add formatter to console handler

    # Add file and console handler to logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger

def main():
    """The main function"""

    # Checking arguments
    args = get_parser().parse_args()
#     check_args(args)

    # Getting arguments
    vcf_files = args.vcf_file.split(',')
    info_jsons = args.info_json.split(',')
    bam_files = args.bam_file.split(',') if args.bam_file else [None]*len(vcf_files)
    qc_type = args.qc_type
    config = args.config
#     resource_dir = os.path.realpath(args.resource_dir)
    output_dir = os.path.realpath(os.path.expanduser(args.output_dir))

    # Setting up logger
    logger = create_logger(args.log_level, output_dir)
    logger.debug('Version: {}'.format(VERSION))

    # Getting executor
    qc_executor = get_qc_executor(qc_type)

    # Running QC
    for i in range(len(vcf_files)):
        # Checking input
        LOGGER.debug('Checking input files "{}" and "{}"'.format(vcf_files[i], info_jsons[i]))
        sample_ids, id_ = check_input(vcf_files[i], info_jsons[i], qc_type)

        # Creating parent output dirs
        LOGGER.debug('Preparing output dirs for files "{}" and "{}"'.format(vcf_files[i], info_jsons[i]))
        qc_outdir_fpath = create_output_dir([output_dir, id_])

        # Copying VCF file and info JSON file in output directory
        LOGGER.debug('Copying VCF file "{}" in output directory "{}"'.format(vcf_files[i], qc_outdir_fpath))
        shutil.copy(vcf_files[i], qc_outdir_fpath)
        LOGGER.debug('Copying info JSON file "{}" in output directory "{}"'.format(info_jsons[i], qc_outdir_fpath))
        shutil.copy(info_jsons[i], qc_outdir_fpath)
        LOGGER.debug('Copying config JSON file "{}" in output directory "{}"'.format(config, qc_outdir_fpath))
        shutil.copy(config, qc_outdir_fpath)

        # Execute QC
        qc_executor(
            vcf_file=vcf_files[i],
            info_file=info_jsons[i],
            bam_file=bam_files[i],
            config=config,
#             resource_dir=resource_dir,
            output_parent_dir=qc_outdir_fpath,
            sample_ids=sample_ids,
            id_=id_
        ).run()

if __name__ == '__main__':
    sys.exit(main())
