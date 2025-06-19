#!/usr/bin/env python3
import json
import os
import logging
import re

import pysam

from utils import execute_bash_command, bgzip_vcf, get_reverse_complement, generate_results_json, list_dir_files

LOGGER = logging.getLogger('variant_qc_logger')

ASSEMBLY = 'GRCh38'
REFERENCE_GENOME_FASTA_FNAME = 'Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz'
TYPE_DEL = "del"
TYPE_TDS = "tds"
TYPE_INV = "inv"
TYPE_TRANS = "trans"
LENGTH_NA = "na"
LENGTH_1_10Kb = "1-10Kb"
LENGTH_10Kb_100Kb = "10-100Kb"
LENGTH_100Kb_1Mb = "100Kb-1Mb"
LENGTH_1Mb_10Mb = "1Mb-10Mb"
LENGTH_10Mb = ">10Mb"
CLUSTERED = 'clustered'
NON_CLUSTERED = 'non-clustered'

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
        self.resource_dir = os.path.realpath(os.path.expanduser(resource_dir))
        self.config_json = config_json
        self.output_dir = output_dir
        self.sample_id = sample_id

        # Getting reference genome file path
        self.ref_genome_fasta_fpath = os.path.join(resource_dir, REFERENCE_GENOME_FASTA_FNAME)

        # Getting variant type from query
        self.ms_type = self.get_ms_type()

        # Expected input files in resource directory
        self.snv_all_vcf_fpath = os.path.join(self.resource_dir, 'SNV_all.vcf.gz')
        self.sv_all_vcf_fpath = os.path.join(self.resource_dir, 'SV_all.vcf.gz')
        self.snv_filtered_vcf_fpath = os.path.join(self.resource_dir, 'SNV_filtered.vcf.gz')\
            if os.path.isfile(os.path.join(self.resource_dir, 'SNV_filtered.vcf.gz')) else self.snv_all_vcf_fpath
        self.sv_filtered_vcf_fpath = os.path.join(self.resource_dir, 'SV_filtered.vcf.gz')\
            if os.path.isfile(os.path.join(self.resource_dir, 'SV_filtered.vcf.gz')) else self.sv_all_vcf_fpath

        # Expected output files
        self.snv_genome_context_fpath = os.path.join(
            self.output_dir, 'OPENCGA_{}_{}_genome_context.csv'.format(self.sample_id, ASSEMBLY)
        )

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

    def create_snv_genome_context_file(self):
        """Create a genome context file that contains all SNVs and their flanking bases

        e.g.
            1:10026:A:C     TAA
            1:10120:T:A     CTA
            1:10126:T:A     CTA
        """

        # Opening ALL SNV VCF file (BGZIP VCF file)
        snv_all_vcf_fpath = bgzip_vcf(self.snv_all_vcf_fpath)  # BGZIPping VCF (pysam requirement)
        pysam_snv_all_vcf_fhand = pysam.VariantFile(snv_all_vcf_fpath)

        # Opening reference genome FASTA file
        ref_genome = pysam.FastaFile(self.ref_genome_fasta_fpath)

        # Creating genome context file to write
        snv_genome_context_fhand = open(self.snv_genome_context_fpath, 'w')

        # Writing context
        flank = 1  # How many bases the variant should be flanked
        for record in pysam_snv_all_vcf_fhand:
            # The position in the vcf file is 1-based, but pysam's fetch() expects 0-base coordinate
            triplet = ref_genome.fetch(record.chrom, record.pos - 1 - flank, record.pos - 1 + len(record.ref) + flank)

            # Writing out contexts as "VARID\tTRIPLET" (1:10026:A:C\tTAA) :
            snv_genome_context_fhand.write(
                '{}:{}:{}:{}\t{}\n'.format(record.chrom, record.pos, record.ref, record.alts[0], triplet)
            )
        snv_genome_context_fhand.close()

    def create_snv_signature_catalogue(self):
        """Create the SNV mutational profile (counts)
        e.g.
        {'counts': [{'context': 'A[C>A]A', 'total': 265},
                    {'context': 'A[C>A]C', 'total': 228},
                    {'context': 'A[C>A]G', 'total': 32},
                    [...]
                    {'context': 'T[T>G]C', 'total': 74},
                    {'context': 'T[T>G]G', 'total': 119},
                    {'context': 'T[T>G]T', 'total': 480}]
        }
        """

        # Getting variant contexts
        snv_genome_context_fhand = open(self.snv_genome_context_fpath, 'r')
        snv_contexts = {line.split()[0]: line.split()[1] for line in snv_genome_context_fhand}
        snv_genome_context_fhand.close()

        # Opening FILTERED SNV VCF file (BGZIP VCF file)
        snv_filtered_vcf_fpath = bgzip_vcf(self.snv_filtered_vcf_fpath)  # BGZIPping VCF (pysam requirement)
        pysam_snv_filtered_vcf_fhand = pysam.VariantFile(snv_filtered_vcf_fpath)

        # Creating context keys
        counts = {}
        for var_mutation in ['[C>A]', '[C>G]', '[C>T]', '[T>A]', '[T>C]', '[T>G]']:
            for first_flanking_base in ['A', 'C', 'G', 'T']:
                for second_flanking_base in ['A', 'C', 'G', 'T']:
                    context_key = ''.join([first_flanking_base, var_mutation, second_flanking_base])
                    counts[context_key] = 0

        # Counting SNV contexts
        for record in pysam_snv_filtered_vcf_fhand:
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
            counts[context_key] += 1

        # Creating results
        results = {'signatures': [{'counts': [{'context': k, 'total': counts[k]} for k in counts],
                                   'id': self.config_json['signatures']['msId'],
                                       'query': self.config_json['signatures']['msQuery'],
                                   'type': self.get_ms_type()}]}
        generate_results_json(results=results, outdir_path=self.output_dir)

    @staticmethod
    def get_sv_type(sv):
        """Get SV type from pysam record

        :param pysam.VariantRecord sv: VCF SV
        :returns: SV type
        """

        # Getting SV type from VCF
        if 'EXT_SVTYPE' in sv.info.keys():
            var_type = sv.info.get('EXT_SVTYPE')
        elif 'SVCLASS' in sv.info.keys():
            var_type = sv.info.get('SVCLASS')
        else:
            var_type = None

        # Translating SV types
        if var_type in ['DEL', 'DELETION']:
            return TYPE_DEL
        elif var_type in ['DUP', 'TDS', 'DUPLICATION', 'TANDEM_DUPLICATION', 'TANDEM-DUPLICATION']:
            return TYPE_TDS
        elif var_type in ['INV', 'INVERSION']:
            return TYPE_INV
        elif var_type in ['TR', 'TRANS', 'TRANSLOCATION']:
            return TYPE_TRANS
        else:
            LOGGER.debug('Skipping variant with unknown type "{}" for the SV mutational catalogue'.format(var_type))
            return None

    @staticmethod
    def get_sv_length(var_type, chrom1, chrom2, pos1, pos2):
        """Get SV length from pysam record

        :param str var_type: SV type
        :param str chrom1: variant chromosome
        :param str chrom2: mate chromosome
        :param int pos1: variant start
        :param int pos2: mate start
        :returns: SV length
        """
        if var_type is None:
            return None
        if var_type == TYPE_TRANS:
            return LENGTH_NA
        else:
            if chrom1 == chrom2:
                length = abs(pos2 - pos1)
                if length <= 10000:
                    return LENGTH_1_10Kb
                elif length <= 100000:
                    return LENGTH_10Kb_100Kb
                elif length <= 1000000:
                    return LENGTH_100Kb_1Mb
                elif length <= 10000000:
                    return LENGTH_1Mb_10Mb
                return LENGTH_10Mb
        return None

    def create_sv_clustered_file(self):
        """Executes R script sv_clustering.R to generate clustering for SVs
        CMD: Rscript sv_clustering.R ./in.clustered.bedpe ./out.clustered.bedpe

        Input file:
        chrom1 start1  end1    chrom2  start2  end2    length  type    sample
        1   100 100 1   200 200 100 del s1
        2   100 100 1   200 200 100 trans   s1
        2   200 200 1   300 300 100 trans   s1

        Output file:
        chrom1	start1	end1	chrom2	start2	end2	length  type    sample	id	is.clustered
        1	100	100	1	200	200	100 del s1  1	FALSE
        2	100	100	1	200	200	100 trans   s1	2	FALSE
        2	200	200	1	300	300	100 trans   s1	3	FALSE

        :returns: The created output dir file
        """

        # Opening ALL SV VCF file (BGZIP VCF file)
        sv_all_vcf_fpath = bgzip_vcf(self.sv_all_vcf_fpath)  # BGZIPping VCF (pysam requirement)
        pysam_sv_all_vcf_fhand = pysam.VariantFile(sv_all_vcf_fpath)

        # Writing input file
        in_bedpe_fpath = os.path.join(self.output_dir, 'in.clustered.bedpe')
        in_bedpe_fhand = open(in_bedpe_fpath, 'w')
        header = ['chrom1', 'start1', 'end1', 'chrom2', 'start2', 'end2', 'length', 'type', 'sample']
        in_bedpe_fhand.write('\t'.join(header) + '\n')
        mate_ids = []
        for record in pysam_sv_all_vcf_fhand:
            # Skipping mates of already visited SVs
            mate_ids.append(record.info.get('MATEID'))
            if record.info.get('VCF_ID') in mate_ids:
                continue

            # Getting variant information
            chrom2, pos2 = re.findall('.*[\[\]](.+):(.+)[\[\]].*', record.alts[0])[0]
            var_type = self.get_sv_type(record)
            var_length = self.get_sv_length(var_type, str(record.chrom), str(chrom2), int(record.pos), int(pos2))
            line = '\t'.join(
                map(str, [record.chrom, record.pos, record.pos, chrom2, pos2, pos2, var_length, var_type, self.sample_id])
            )
            in_bedpe_fhand.write(line + '\n')
        in_bedpe_fhand.close()

        # Executing SV clustering
        r_script = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'sv_clustering.R')
        out_bedpe_fpath = os.path.join(self.output_dir, 'out.clustered.bedpe')
        cmd = 'Rscript {} {} {}'.format(r_script, in_bedpe_fpath, out_bedpe_fpath)
        execute_bash_command(cmd)

        return out_bedpe_fpath

    def create_sv_signature_catalogue(self):
        """Create the SV mutational profile (counts)
        e.g.
        {'counts': [{'context': 'clustered_del_1-10Kb', 'total': 4},
                    {'context': 'clustered_del_10-100Kb', 'total': 3},
                    [...]
                    {'context': 'clustered_trans', 'total': 82},
                    [...]
                    {'context': 'non-clustered_del_1-10Kb', 'total': 26},
                    {'context': 'non-clustered_del_10-100Kb', 'total': 25},
                    [...]
                    {'context': 'non-clustered_trans', 'total': 49}]
        }
        """

        # Creating context keys
        counts = {}
        for var_clustering in [CLUSTERED, NON_CLUSTERED]:
            for var_type in [TYPE_DEL, TYPE_TDS, TYPE_INV, TYPE_TRANS]:
                for length in [LENGTH_1_10Kb, LENGTH_10Kb_100Kb, LENGTH_100Kb_1Mb, LENGTH_1Mb_10Mb, LENGTH_10Mb]:
                    if var_type == TYPE_TRANS:
                        context_key = '_'.join([var_clustering, var_type])
                    else:
                        context_key = '_'.join([var_clustering, var_type, length])
                    counts[context_key] = 0

        # Counting SV contexts
        out_bedpe_fpath = self.create_sv_clustered_file()
        out_bedpe_fhand = open(out_bedpe_fpath, 'r')
        out_bedpe_fhand.readline()  # Skipping header
        for line in out_bedpe_fhand:
            items = line.split()
            var_clustering = CLUSTERED if items[10] == 'TRUE' else NON_CLUSTERED
            var_type = items[7]
            var_length = items[6]
            if var_type == TYPE_TRANS:
                context_key = '_'.join([var_clustering, var_type])
            else:
                context_key = '_'.join([var_clustering, var_type, var_length])
            counts[context_key] += 1

        # Creating results
        results = {'signatures': [{'counts': [{'context': k, 'total': counts[k]} for k in counts],
                                   'id': self.config_json['signatures']['msId'],
                                       'query': self.config_json['signatures']['msQuery'],
                                   'type': self.get_ms_type()}]}
        generate_results_json(results=results, outdir_path=self.output_dir)

    def get_fitting_command(self):

        # Creating basic CMD
        cmd = 'R CMD Rscript --vanilla'
        cmd += ' /opt/opencga/signature.tools.lib/scripts/signatureFit'
        cmd += ' --commonsigtier=T2'
        cmd += ' --genomev=hg38'

        # Creating mutational catalogue counts file and adding it as parameter
        if 'results.json' in list_dir_files(self.output_dir):
            # Opening input/output files
            results_fhand = open(os.path.join(self.output_dir, 'results.json'), 'r')
            catalogues_fpath = os.path.join(self.output_dir, 'catalogues.tsv')
            catalogues_fhand = open(catalogues_fpath, 'w')
            # Creating catalogue file
            results_json = json.loads(results_fhand.read())
            for count in results_json['signatures'][0]['counts']:
                catalogues_fhand.write('{}\t{}\n'.format(count['context'], count['total']))
        elif 'catalogues.tsv' in list_dir_files(self.resource_dir):
            catalogues_fpath = os.path.join(self.resource_dir, 'catalogues.tsv')
        else:
            raise ValueError('No catalogue file found for fitting analysis')
        cmd += ' --catalogues={}'.format(catalogues_fpath)

        # Adding output dir
        cmd += ' --outdir={}'.format(self.output_dir)

        # Adding extra analysis parameters
        # --fitmethod
        try:
            cmd += ' --fitmethod={}'.format(self.config_json['signatures']['msQuery']['msFitMethod'])
        except KeyError:
            pass
        # --sigversion
        try:
            cmd += ' --sigversion={}'.format(self.config_json['signatures']['msQuery']['msFitSigVersion'])
        except KeyError:
            pass
        # --organ
        try:
            cmd += ' --organ={}'.format(self.config_json['signatures']['msQuery']['msFitOrgan'])
        except KeyError:
            pass
        # --thresholdperc
        try:
            cmd += ' --thresholdperc={}'.format(self.config_json['signatures']['msQuery']['msFitThresholdPerc'])
        except KeyError:
            pass
        # --thresholdpval
        try:
            cmd += ' --thresholdpval={}'.format(self.config_json['signatures']['msQuery']['msFitThresholdPval'])
        except KeyError:
            pass
        # --maxraresigs
        try:
            cmd += ' --maxraresigs={}'.format(self.config_json['signatures']['msQuery']['msFitMaxRareSigs'])
        except KeyError:
            pass
        # --nboot
        try:
            cmd += ' --nboot={}'.format(self.config_json['signatures']['msQuery']['msFitNBoot'])
        except KeyError:
            pass
        # --signaturesfile
        try:
            signatures_file_fpath = os.path.join(self.resource_dir,
                                                 self.config_json['signatures']['msQuery']['msFitSignaturesFile'])
            cmd += ' --signaturesfile={}'.format(signatures_file_fpath)
        except KeyError:
            pass
        # --raresignaturesfile
        try:
            rare_signatures_file_fpath = os.path.join(self.resource_dir,
                                                      self.config_json['signatures']['msQuery']['msFitRareSignaturesFile'])
            cmd += ' --raresignaturesfile={}'.format(rare_signatures_file_fpath)
        except KeyError:
            pass

        return cmd

    def create_signature_fitting(self):
        cmd = self.get_fitting_command()
        execute_bash_command(cmd)


        # TODO Write results


    def run(self):
        # Creating mutational signature catalogue
        if self.ms_type == 'SNV':
            self.create_snv_genome_context_file()
            self.create_snv_signature_catalogue()
            self.create_signature_fitting()
        elif self.ms_type == 'SV':
            self.create_sv_signature_catalogue()
            self.create_signature_fitting()
            pass
        else:
            msg = 'Mutational signature for type "{}" not implemented'.format(self.ms_type)
            raise ValueError(msg)




