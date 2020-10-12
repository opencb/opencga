#!/usr/bin/env python

import sys
import argparse


_SEX = {
    '0': 'UNKNOWN',
    '1': 'MALE',
    '2': 'FEMALE'
}

_KAR_SEX = {
    '0': None,
    '1': 'XY',
    '2': 'XX'
}
_VARIABLE_FIELDS = ['Relationship', 'Siblings', 'Second Order', 'Third Order', 'Other Comments']

FNAME_TEMPLATE = 'ALL.chr{}.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf.gz'


def to_camel_case(text):
    components = text.lower().replace('_', ' ').split(' ')
    return components[0].lower() + ''.join(x.title() for x in components[1:])


def create_variable_sets(header):
    text = []
    text.append('variableSets:')
    text.append('{}- id: relation'.format(' '*2))
    text.append('{}name: relation'.format(' '*4))
    text.append('{}entities:'.format(' '*4))
    text.append('{}- INDIVIDUAL'.format(' '*6))
    text.append('{}variables:'.format(' '*4))
    for field in header:
        if field not in _VARIABLE_FIELDS:
            continue
        text.append('{}- id: {}'.format(' '*6, to_camel_case(field)))
        text.append('{}name: {}'.format(' '*8, to_camel_case(field)))
        text.append('{}type: STRING'.format(' '*8))
    return '\n'.join(text)


def create_individuals(ind_info):
    text = []
    text.append('individuals:')
    for ind in ind_info:
        text.append('{}- id: {}'.format(' '*2, ind['Individual ID']))
        text.append('{}name: {}'.format(' '*4, ind['Individual ID']))
        if ind['Paternal ID'] != '0':
            text.append('{}father:'.format(' '*4))
            text.append('{}id: {}'.format(' '*6, ind['Paternal ID']))
        if ind['Maternal ID'] != '0':
            text.append('{}mother:'.format(' '*4))
            text.append('{}id: {}'.format(' '*6, ind['Maternal ID']))
        text.append('{}sex: {}'.format(' '*4, _SEX[ind['Gender']]))
        text.append('{}karyotypicSex: {}'.format(' '*4, _KAR_SEX[ind['Gender']]))
        text.append('{}population:'.format(' '*4))
        text.append('{}name: {}'.format(' '*6, ind['Population']))

        text.append('{}annotationSets:'.format(' '*4))
        text.append('{}- id: relation'.format(' '*6))
        text.append('{}name: relation'.format(' '*8))
        text.append('{}variableSetId: relation'.format(' '*8, ind['Population']))
        text.append('{}annotations:'.format(' '*8))
        for field in ind.keys():
            if field not in _VARIABLE_FIELDS:
                continue
            text.append('{}{}: {}'.format(' '*10, to_camel_case(field), ind[field]))
        text.append('{}samples:'.format(' '*4))
        text.append('{}- id: {}'.format(' '*6, ind['Individual ID']))
    return '\n'.join(text)


def create_samples(ind_info):
    text = []
    text.append('samples:')
    for ind in ind_info:
        text.append('{}- id: {}'.format(' '*2, ind['Individual ID']))
        text.append('{}individualId: {}'.format(' '*4, ind['Individual ID']))
    return '\n'.join(text)


def create_families(ind_info):
    families = {}
    for ind in ind_info:
        if ind['Family ID'] != ind['Individual ID']:
            families.setdefault(ind['Family ID'], []).append(ind['Individual ID'])
            for member in [ind['Individual ID'], ind['Paternal ID'], ind['Maternal ID']]:
                if member != '0':
                    families[ind['Family ID']].append(member)

    for family in families:
        families[family] = list(set(families[family]))

    text = []
    text.append('families:')
    for family in families:
        text.append('{}- id: {}'.format(' '*2, family))
        text.append('{}name: {}'.format(' '*4, family))
        text.append('{}members:'.format(' '*4))
        for member in families[family]:
            text.append('{}- id: {}'.format(' '*6, member))
    return '\n'.join(text)


def create_files():
    text = []
    text.append('files:')
    for chrom in list(range(1, 23)) + ['X']:
        text.append('{}- name: {}'.format(' '*2, FNAME_TEMPLATE.format(chrom)))
        text.append('{}path: {}'.format(' '*4, 'data'))
    return '\n'.join(text)


def create_attributes():
    text = []
    text.append('attributes:')
    text.append('{}variant-index-run:'.format(' '*2))
    text.append('{}loadArchive: NO'.format(' '*4))
    text.append('{}loadHomRef: NO'.format(' '*4))
    text.append('{}loadSplitData: CHROMOSOME'.format(' '*4))
    return '\n'.join(text)


def _setup_argparse():
    desc = 'This script creates automatically all Python RestClients files'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('ped_file', help='Pedigree file path')
    parser.add_argument('outfile', help='Output file path')
    args = parser.parse_args()
    return args


def main():

    args = _setup_argparse()

    ped_fhand = open(args.ped_file, 'r')
    yml_fhand = open(args.outfile, 'w')

    header = ped_fhand.readline().strip().split('\t')
    ind_info = [{k: v for k, v in zip(header, line.strip().split('\t'))} for line in ped_fhand]

    yml_fhand.write('---\n')
    yml_fhand.write('# WARNING: AUTOGENERATED CONTENT\n')
    yml_fhand.write('id: 1000G\n')
    yml_fhand.write('name: 1000 Genomes phase 3\n')
    yml_fhand.write('description: The 1000 Genomes Project\n')
    yml_fhand.write(create_variable_sets(header) + '\n')
    yml_fhand.write(create_individuals(ind_info) + '\n')
    yml_fhand.write(create_families(ind_info) + '\n')
    yml_fhand.write(create_files() + '\n')
    yml_fhand.write(create_attributes() + '\n')

    ped_fhand.close()
    yml_fhand.close()


if __name__ == '__main__':
    sys.exit(main())
