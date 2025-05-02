#!/usr/bin/env python3

import re
import matplotlib.pyplot as plt

def parse_vcfstats(filename):
    data = {}
    with open(filename, 'r') as file:
        section = None
        for line in file:
            line = line.strip()
            if line.startswith("SN"):
                match = re.match(r'^SN\s+(\S+)\s+(\d+)', line)
                if match:
                    key = match.group(1)
                    value = int(match.group(2))
                    data.setdefault('SN', {})[key] = value
            elif line.startswith("TSTV"):
                match = re.match(r'^TSTV\s+(\S+)\s+(\d+)', line)
                if match:
                    key = match.group(1)
                    value = int(match.group(2))
                    data.setdefault('TSTV', {})[key] = value
            elif line.startswith("PSC"):
                match = re.match(r'^PSC\s+(\S+)\s+(\d+)', line)
                if match:
                    key = match.group(1)
                    value = int(match.group(2))
                    data.setdefault('PSC', {})[key] = value
            # Add more sections as needed
    return data


def plot_data(data, prefix):
    sn_data = data.get('SN', {})
    keys = list(sn_data.keys())
    values = list(sn_data.values())

    plt.figure(figsize=(10, 6))
    plt.barh(keys, values)
    plt.xlabel('Count')
    plt.title('SN Statistics')
    plt.tight_layout()
    plt.savefig(f'{prefix}_sn_statistics.png')
    plt.close()

    # Add additional plots for TSTV, PSC, etc., based on the data sections

def plot_bcftools_stats(file, prefix, output):
    all_data = {}
    data = parse_vcfstats(file)
    for section, values in data.items():
        if section not in all_data:
            all_data[section] = values
        else:
            for key, value in values.items():
                all_data[section][key] = all_data[section].get(key, 0) + value

    plot_data(all_data, output + prefix)
