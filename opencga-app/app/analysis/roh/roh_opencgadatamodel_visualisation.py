import os
import sys
import argparse
import json
import matplotlib.pyplot as plt
import numpy as np

def main():
    parser=argparse.ArgumentParser(description='''Visualising ROH. ''')
    parser.add_argument('-i','--input_folder_path', help='Full path to all data input folder',required=True)
    parser.add_argument('-p','--files_prefix', help='Input files prefix',required=True)
    parser.add_argument('-j','--json_opencga_roh', help='JSON string with opencga roh data model.',required=True)
    parser.add_argument('-o','--output_folder_path', help='Full path to output folder',required=True)
    args=parser.parse_args()


    #_______________________________Initialising arguments__________________________#
    input_folder_fpath = args.input_folder_path
    files_prefix = args.files_prefix
    if '.json' in args.json_opencga_roh: 
        input_opencga_roh = json.load(open(''.join([input_folder_fpath,'/','opencga_roh_analysis_method_data_model.json']), 'r'))
    else:
        input_opencga_roh = json.loads(args.json_opencga_roh)
    template_roh_json_data_model = input_opencga_roh
    output_folder = args.output_folder_path
    # Input files
    input_roh_hom_summary_fhand = open(''.join([input_folder_fpath,'/',files_prefix,'.hom.summary']), 'r')
    input_roh_hom_fhand = open(''.join([input_folder_fpath,'/',files_prefix,'.hom']), 'r')
    input_freq_fhand = open(''.join([input_folder_fpath,'/',files_prefix,'.frq']), 'r')
    input_fam_fhand = open(''.join([input_folder_fpath,'/',files_prefix,'.fam']), 'r')


    #_______________________________Data preparation_______________________________#
    for line in input_fam_fhand:
        columns_fam = line.strip().split()
        # Getting sample id
        sample = str(columns_fam[1])
    
    # Format of dict_positions = {variant_id:(alt_allele_freq,roh_presence)}
    dict_positions = {}
    for index,line in enumerate(input_freq_fhand):
        if index != 0:
            columns_freq=line.strip().split()
            var_id = columns_freq[1]
            dict_positions[var_id] = {}
            alt_allele=(var_id.split(":"))[3]
            # If true, the minor allele is the alternate allele
            if alt_allele == columns_freq[2]:
                alt_allele_freq = float(columns_freq[4])
            else:
                alt_allele_freq = 1 - float(columns_freq[4])
            dict_positions[var_id] = alt_allele_freq
            if index == 1:
                chromosome = str(columns_freq[0])

    # Obtaining all values in column UNAFF of .hom.summary file
    for index,line in enumerate(input_roh_hom_summary_fhand):
        columns_hom_summary = line.strip().split()
        if index != 0:
            alt_allele_freq = dict_positions[columns_hom_summary[1]]
            dict_positions[columns_hom_summary[1]] = (alt_allele_freq,int(columns_hom_summary[4]))

    # Filling roh_boundaries and roh_regions_results lists by reading .hom file
    roh_regions_results = []
    roh_boundaries = []
    for index,line in enumerate(input_roh_hom_fhand):
        columns_hom = line.strip().split()
        if index != 0:
            roh_boundaries.extend([int(columns_hom[6]),int(columns_hom[7])])
            roh_region = {  
                "chr": int(columns_hom[3]),
                "start": int(columns_hom[6]),
                "end": int(columns_hom[7]),
                "kb": float(columns_hom[8]),
                "nsnp": int(columns_hom[9]),
                "density": float(columns_hom[10]),
                "phom": float(columns_hom[11]),
                "phet": float(columns_hom[12])
                }
            roh_regions_results.append(roh_region)


#_______________________________OpenCGA JSON Generation____________________________#
    template_roh_json_data_model["roh"][0]["id"] = sample
    template_roh_json_data_model["roh"][0]["regions"] = roh_regions_results
    roh_analysis_sample_data_model_file_name=('_'.join([files_prefix,'opencga_roh_analysis_sample_data_model.json']))
    json.dump(template_roh_json_data_model,open(os.path.join(output_folder,roh_analysis_sample_data_model_file_name),'w'))            
    os.remove(os.path.join(input_folder_fpath,'opencga_roh_analysis_method_data_model.json'))

#_______________________________Data visualisation_________________________________#
    bp_list_roh = []
    alt_allele_frequency_list_roh = []
    bp_list_rest = []
    alt_allele_frequency_list_rest = []
    for id,values in dict_positions.items():
        position = int((id.split(":"))[1])
        if values[1] == 1:
            bp_list_roh.append(position)
            alt_allele_frequency_list_roh.append(values[0])
        else:
            bp_list_rest.append(position)
            alt_allele_frequency_list_rest.append(values[0])
    # Creating the scatterplot: x-axis bp and y-axis alt_allele_freq
    output_folder_file_plot = ''.join([output_folder,"/",files_prefix,"_alt_allele_freq_roh.png"])
    plt.scatter(bp_list_rest,alt_allele_frequency_list_rest,color="gray",marker=".",label="Other regions",s=5,alpha=1.0)
    plt.scatter(bp_list_roh,alt_allele_frequency_list_roh,color="blue",marker=".",label="ROH",s=5)
    if len(roh_boundaries) > 0:
        vertical_lines = roh_boundaries
        # other colours in https://matplotlib.org/stable/gallery/color/named_colors.html
        plt.vlines(x=vertical_lines,colors="lavender",ymin=-0.1,ymax=1.1,linestyles='dashed')
    # y ticks from 0 to 1 every 0.1
    plt.yticks(np.arange(0, 1.1, step=0.1))
    for frame_line_pos in ['right', 'top', 'bottom', 'left']:
        plt.gca().spines[frame_line_pos].set_visible(False)
    title = sample + ": SNPs in chr" + chromosome
    plt.title(title)
    x_label = "SNPs location in chr" + chromosome + " (bp)"
    plt.xlabel(x_label)
    plt.ylabel("Alternate allele frequency")
    # loc=9 indicates the upper centre position
    plt.legend(loc=9,bbox_to_anchor=(0.5,1.17),ncol=2)
    plt.savefig(output_folder_file_plot)


if __name__ == '__main__':
    sys.exit(main())