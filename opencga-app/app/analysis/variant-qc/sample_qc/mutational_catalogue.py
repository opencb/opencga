#!/usr/bin/env python3

import sys
import os
import logging
import json
import gzip
import re

import pysam

from utils import create_output_dir, execute_bash_command, bgzip_vcf, get_reverse_complement, generate_results_json

LOGGER = logging.getLogger('variant_qc_logger')

REFERENCE_GENOME_FASTA_FNAME = 'Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz'


class MutationalCatalogueAnalysis:
    def __init__(self, vcf_file, resource_dir, config_json, output_dir, sample_id):
        """Create output dir

        :param str vcf_file: VCF input file path
        :param str resource_dir: Output directory path for resources
        :param dict config_json: Configuration JSON
        :param str output_dir: Output directory path for this analysis
        :param str sample_id: Sample ID
        """
        self.vcf_file = vcf_file
        self.resource_dir = resource_dir
        self.config_json = config_json
        self.output_dir = output_dir
        self.sample_id = sample_id

        # Getting reference genome file path
        self.ref_genome_fasta_fpath = os.path.join(resource_dir, REFERENCE_GENOME_FASTA_FNAME)

        # Getting variant type from query
        self.ms_type = self.get_ms_type()

        # Intermediate files
        self.snv_vcf_fpath = os.path.join(self.output_dir,
                                          'SNV-' + re.sub('\.gz$', '', os.path.basename(self.vcf_file)) + '.bgz')
        self.sv_vcf_fpath = os.path.join(self.output_dir,
                                         'SV-' + re.sub('\.gz$', '', os.path.basename(self.vcf_file)) + '.bgz')
        self.snv_genome_context_fpath = os.path.join(self.output_dir,
                                                     'OPENCGA_{}_GRCh38_genome_context.csv'.format(self.sample_id))

        # CONFIG
        #     "msId": "",
        #     "msDescription": "",
        #     "msQuery": "",    # "query": "{\"fileData\":\"" + INDIVIDUALS[i] + ".annot.muts.caveman.vcf.gz:FILTER=PASS;CLPM<=0;ASMD>=140\", \"region\": \"1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,X,Y\", \"type\":\"SNV\"}",
        #
        #     "msFitId": "",
        #     "msFitMethod": "",
        #     "msFitNBoot": 0,
        #     "msFitSigVersion": "",
        #     "msFitOrgan": "",
        #     "msFitThresholdPerc": {},
        #     "msFitThresholdPval": {},
        #     "msFitMaxRareSigs": 0,
        #     "msFitSignaturesFile": "",
        #     "msFitRareSignaturesFile": "",

    def get_ms_type(self):
        ms_type = None
        if ('signatures' in self.config_json and self.config_json['signatures']
                and 'msQuery' in self.config_json['signatures'] and self.config_json['signatures']['msQuery']
                and 'type' in self.config_json['signatures']['msQuery'] and self.config_json['signatures']['msQuery']['type']):
            ms_type = self.config_json['signatures']['msQuery']['type']
        return ms_type

    @staticmethod
    def vcf_filter_iterator(vcf_fpath, opencga_query, header=True):
        """Filter a VCF given an opencga query

        :param str vcf_fpath: VCF input file path
        :param dict opencga_query: Output directory path for resources
        :param bool header: Include VCF header if true
        :returns: VCF variants
        """
        # BGZIPping VCF (pysam requirement)
        vcf_fpath = bgzip_vcf(vcf_fpath)

        # Translating variant types
        type_ = []
        if 'type' in opencga_query:
            for t in opencga_query['type'].split(','):
                if t == 'SNV':
                    type_ += ['Sub']
                if t == 'INDEL':
                    type_ += ['Del', 'Ins']
                if t == 'SV':
                    type_ += ['BND', 'DELETION', 'BREAKEND', 'DUPLICATION', 'TANDEM_DUPLICATION', 'INVERSION', 'TRANSLOCATION']
                if t == 'CNV':
                    type_ += ['CNV', 'COPY_NUMBER']

        # Opening VCF file (BGZIP VCF file)
        pysam_vcf_fhand = pysam.VariantFile(vcf_fpath)

        # FILTERING
        if header:
            yield pysam_vcf_fhand.header
        for record in pysam_vcf_fhand:
            # Getting all VCF types
            vcf_types = list(filter(None, [record.info.get('VT'), record.info.get('EXT_SVTYPE'), record.info.get('SVTYPE')]))
            if not set(vcf_types).intersection(type_):
                continue

            yield record

    def create_snv_genome_context_file(self):
        """Create a genome context file that contains all SNVs and their flanking bases

        e.g.
            1:10026:A:C     TAA
            1:10120:T:A     CTA
            1:10126:T:A     CTA
        """

        # TODO Is this already provided and we just have to take it from a folder?
        # Create VCF file with ALL SNVs
        snv_vcf_fhand = open(self.snv_vcf_fpath[:-4], 'w')
        for variant in self.vcf_filter_iterator(vcf_fpath=self.vcf_file, opencga_query={"type": "SNV"}):
            snv_vcf_fhand.write(str(variant))
        snv_vcf_fhand.close()
        self.snv_vcf_fpath = bgzip_vcf(self.snv_vcf_fpath[:-4], delete_original=True)  # BGZIPping VCF (pysam req)

        # Opening SNV VCF file (BGZIP VCF file)
        pysam_snv_vcf_fhand = pysam.VariantFile(self.snv_vcf_fpath)

        # Opening reference genome FASTA file
        ref_genome = pysam.FastaFile(self.ref_genome_fasta_fpath)

        # Creating genome context file to write
        snv_genome_context_fhand = open(self.snv_genome_context_fpath, 'w')

        # Writing context
        flank = 1  # How many bases the variant should be flanked
        for record in pysam_snv_vcf_fhand:
            # The position in the vcf file is 1-based, but pysam's fetch() expects 0-base coordinate
            triplet = ref_genome.fetch(record.chrom, record.pos - 1 - flank, record.pos - 1 + len(record.ref) + flank)

            # Writing out contexts as "VARID\tTRIPLET" (1:10026:A:C\tTAA) :
            snv_genome_context_fhand.write(
                '{}:{}:{}:{}\t{}\n'.format(record.chrom, record.pos, record.ref, record.alts[0], triplet)
            )
        snv_genome_context_fhand.close()

    def create_snv_signature_catalogue(self):

        # Getting variant contexts
        snv_genome_context_fhand = open(self.snv_genome_context_fpath, 'r')
        snv_contexts = {line.split()[0]: line.split()[1] for line in snv_genome_context_fhand}
        snv_genome_context_fhand.close()

        # TODO Filter SNV VCF to get queried SNVs - Is this already provided and we just have to take it from a folder?

        # Opening SNV VCF file (BGZIP VCF file)
        pysam_snv_vcf_fhand = pysam.VariantFile(self.snv_vcf_fpath)

        # Counting SNV contexts
        counts = {}
        for record in pysam_snv_vcf_fhand:
            var_id = ':'.join(map(str, [record.chrom, record.pos, record.ref, ','.join(list(record.alts))]))

            # Creating key "first_flanking_base[REF>ALT]second_flanking_base". e.g. "A[C>A]T"
            # Reverse complement contexts whose first flanking base is not "C" or "T"
            # Main groups in SNV mutational profiles: C>A, C>G, C>T, T>A, T>C, T>G
            if record.ref not in ['C', 'T']:
                context = get_reverse_complement(snv_contexts[var_id])
                alt = get_reverse_complement(var_id.split(':')[3])
            else:
                context = snv_contexts[var_id]
                alt = var_id.split(':')[3]
            context_key = '{}[{}>{}]{}'.format(context[0], context[1], alt, context[2])
            counts[context_key] = counts.get(context_key, 0) + 1

        # Creating results
        results = {'signatures': [{'counts': [{'context': k, 'total': counts[k]} for k in counts]}]}
        generate_results_json(results=results, outdir_path=self.output_dir)

    def create_sv_clustering(self, sv_vcf_fpath):
        """Executes R script sv_clustering.R to generate clustering for SVs
        CMD: Rscript sv_clustering.R ./in.bedpe ./out.bedpe

        Input file:
        chrom1  start1  end1    chrom2  start2  end2    sample
        1   100 100 1   200 200 s1
        2   100 100 1   200 200 s1
        2   200 200 1   300 300 s1

        Output file:
        chrom1	start1	end1	chrom2	start2	end2	sample	id	is.clustered
        1	100	100	1	200	200	s1	1	FALSE
        2	100	100	1	200	200	s1	2	FALSE
        2	200	200	1	300	300	s1	3	FALSE

        :param str sv_vcf_fpath: SV VCF input file path
        :returns: The created output dir file
        """

        # Opening SV VCF file (BGZIP VCF file)
        pysam_sv_vcf_fhand = pysam.VariantFile(sv_vcf_fpath)

        # Writing input file
        in_bedpe_fpath = os.path.join(self.output_dir, 'in.bedpe')
        in_bedpe_fhand = open(in_bedpe_fpath, 'w')
        in_bedpe_fhand.write('\t'.join(['chrom1', 'start1', 'end1', 'chrom2', 'start2', 'end2', 'sample']) + '\n')
        mate_ids = []
        for record in pysam_sv_vcf_fhand:
            mate_ids.append(record.info.get('MATEID'))
            if record.info.get('VCF_ID') in mate_ids:  # Skip mates of already visited SVs
                continue
            chrom2, pos2 = re.findall('.*[\[\]](.+):(.+)[\[\]].*', record.alts[0])[0]
            line = '\t'.join(map(str, [record.chrom, record.pos, record.pos, chrom2, pos2, pos2, self.sample_id]))
            in_bedpe_fhand.write(line + '\n')
        in_bedpe_fhand.close()

        # Executing SV clustering
        r_script = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'sv_clustering.R')
        out_bedpe_fpath = os.path.join(self.output_dir, 'out.bedpe')
        cmd = 'Rscript {} {} {}'.format(r_script, in_bedpe_fpath, out_bedpe_fpath)
        execute_bash_command(cmd)

        return out_bedpe_fpath

    def create_sv_clustered_context_file(self):

        # TODO Is this already provided and we just have to take it from a folder?
        # Create VCF file with ALL SVs
        sv_vcf_fhand = open(self.sv_vcf_fpath[:-4], 'w')
        for variant in self.vcf_filter_iterator(vcf_fpath=self.vcf_file, opencga_query={"type": "SV"}):
            sv_vcf_fhand.write(str(variant))
        sv_vcf_fhand.close()
        self.sv_vcf_fpath = bgzip_vcf(self.sv_vcf_fpath[:-4], delete_original=True)  # BGZIPping VCF (pysam req)

        # Generating clustering for SVs
        out_bedpe_fpath = self.create_sv_clustering(self.sv_vcf_fpath)

        # Counting SV contexts
        # https://cancer.sanger.ac.uk/signatures/sv/sv1/
        # TODO


    def run(self):
        # Creating mutational signature catalogue
        if self.ms_type == 'SNV':
            self.create_snv_genome_context_file()
            self.create_snv_signature_catalogue()
        elif self.ms_type == 'SV':
            self.create_sv_clustered_context_file()
            # self.create_sv_signature_catalogue()
            pass
        else:
            msg = 'Mutational signature for type "{}" not implemented'.format(self.ms_type)
            raise ValueError(msg)




