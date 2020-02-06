#!/usr/bin/env python

import sys


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

def create_individuals(ind_info, yml_fhand):
    text = []
    text.append('individuals:')
    for ind in ind_info:
        text.append('{}- id: {}'.format(' '*2, ind['Individual ID']))
        text.append('{}name: {}'.format(' '*4, ind['Individual ID']))
        if ind['Paternal ID'] != '0':
            text.append('{}father:'.format(' '*4))
            text.append('{}- id: {}'.format(' '*6, ind['Paternal ID']))
        if ind['Maternal ID'] != '0':
            text.append('{}mother:'.format(' '*4))
            text.append('{}- id: {}'.format(' '*6, ind['Maternal ID']))
        text.append('{}sex: {}'.format(' '*4, _SEX[ind['Gender']]))
        text.append('{}karyotypicSex: {}'.format(' '*4, _KAR_SEX[ind['Gender']]))
        text.append('{}lifeStatus: {}'.format(' '*4, 'UNKNOWN'))
        text.append('{}population: {}'.format(' '*4, ind['Population']))
    return '\n'.join(text)

def create_samples(ind_info, yml_fhand):
    text = []
    text.append('samples:')
    for ind in ind_info:
        text.append('{}- id: {}'.format(' '*2, ind['Individual ID']))
        text.append('{}source: {}'.format(' '*4, 'UNKNOWN'))
        text.append('{}individualId: {}'.format(' '*4, ind['Individual ID']))
    return '\n'.join(text)

def create_cohorts(ind_info, yml_fhand):
    text = []
    text.append('cohorts:')
    text.append('{}- id: {}'.format(' '*2, 'ALL'))
    text.append('{}samples: {}'.format(' '*4, 'UNKNOWN'))
    for ind in ind_info:
        text.append('{}- id: {}'.format(' '*6, ind['Individual ID']))
    return '\n'.join(text)

def main():
    ped_fpath = sys.argv[1]
    yml_fpath = sys.argv[2]

    ped_fhand = open(ped_fpath, 'r')
    yml_fhand = open(yml_fpath, 'w')

    header = ped_fhand.readline().strip().split('\t')
    ind_info = [{k: v for k, v in zip(header, line.strip().split('\t'))} for line in ped_fhand]

    yml_fhand.write('---\n')
    yml_fhand.write('id: 1kg\n')
    yml_fhand.write(create_individuals(ind_info, yml_fhand) + '\n')
    yml_fhand.write(create_samples(ind_info, yml_fhand) + '\n')
    yml_fhand.write(create_cohorts(ind_info, yml_fhand) + '\n')

    ped_fhand.close()
    yml_fhand.close()


if __name__ == '__main__':
    sys.exit(main())