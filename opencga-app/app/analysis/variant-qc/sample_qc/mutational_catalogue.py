#!/usr/bin/env python3
import json
import os
import logging
import re
import shlex

import pysam

from utils import execute_bash_command, bgzip_vcf, get_reverse_complement, generate_results_json, list_dir_files, list_dir_dirs, convert_to_base64

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
CATALOGUES_FILENAME_DEFAULT = 'catalogues.tsv'
SIGNATURE_COEFFS_FILENAME = 'exposures.tsv'

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

    def get_ms_type(self):
        """Get mutation type"""
        ms_type = None
        if ('msQuery' in self.config_json and self.config_json['msQuery']
                and 'type' in self.config_json['msQuery'] and self.config_json['msQuery']['type']):
            ms_type = self.config_json['msQuery']['type']
        return ms_type

    def create_snv_genome_context_file(self):
        """Create a genome context file that contains all SNVs and their flanking bases

        e.g.
            1:10026:A:C     TAA
            1:10120:T:A     CTA
            1:10126:T:A     CTA
        """
        LOGGER.info('Creating the SNV genome context file "{}"'.format(self.snv_genome_context_fpath))

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
        LOGGER.info('Creating the SNV signature catalogue context file')

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
        results = {'query': self.config_json['msQuery'],
                   'type': self.get_ms_type(),
                   'counts': [{'context': k, 'total': counts[k]} for k in counts]}
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
            LOGGER.warning('Skipping variant with unknown type "{}" for the SV mutational catalogue'.format(var_type))
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
        LOGGER.info('Creating the SV signature catalogue context file')

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
        results = {'query': self.config_json['msQuery'],
                   'type': self.get_ms_type(),
                   'counts': [{'context': k, 'total': counts[k]} for k in counts]}
        generate_results_json(results=results, outdir_path=self.output_dir)

    def get_fitting_command(self):
        """Create the CMD to run mutational signature fitting

        Running this CMD will generate a file named 'exposures.tsv' with coefficient keys in the first row and their
        values in the second. NOTE: the second row (values) starts with the name of the sample.
        e.g.
        SBS3    unassigned
        HCC1954 70.6666249913731        29.3333750086269

        :returns: The CMD to run the signature fitting
        """

        # Creating basic CMD and params dict
        cmd = 'R CMD Rscript --vanilla /opt/opencga/signature.tools.lib/scripts/signatureFit'
        params = {}

        # Adding common signature tier
        params['commonsigtier'] = 'T2'

        # Adding genome reference
        params['genomev'] = 'hg38'

        # Creating mutational catalogue counts file and adding it as parameter
        if 'results.json' in list_dir_files(self.output_dir):
            # Opening input/output files
            results_fhand = open(os.path.join(self.output_dir, 'results.json'), 'r')
            catalogues_fpath = os.path.join(self.output_dir, CATALOGUES_FILENAME_DEFAULT)
            catalogues_fhand = open(catalogues_fpath, 'w')
            # Creating catalogue file
            results_json = json.loads(results_fhand.read())
            catalogues_fhand.write('{}\n'.format(self.sample_id))
            for count in results_json['counts']:
                catalogues_fhand.write('{}\t{}\n'.format(count['context'], count['total']))
            # Closing input/output files
            results_fhand.close()
            catalogues_fhand.close()
        elif CATALOGUES_FILENAME_DEFAULT in list_dir_files(self.resource_dir):
            catalogues_fpath = os.path.join(self.resource_dir, CATALOGUES_FILENAME_DEFAULT)
        else:
            raise ValueError('No catalogue file "{}" found for fitting analysis'.format(CATALOGUES_FILENAME_DEFAULT))
        params['catalogues'] = catalogues_fpath

        # Adding output dir
        params['outdir'] = self.output_dir

        # Adding extra analysis parameters
        # --fitmethod
        if 'msFitMethod' in self.config_json and self.config_json['msFitMethod']:
            params['fitmethod'] = self.config_json['msFitMethod']
        # --sigversion
        if 'msFitSigVersion' in self.config_json and self.config_json['msFitSigVersion']:
            params['sigversion'] = self.config_json['msFitSigVersion']
        # --organ
        if 'msFitOrgan' in self.config_json and self.config_json['msFitOrgan']:
            params['organ'] = self.config_json['msFitOrgan']
        # --thresholdperc
        if 'msFitThresholdPerc' in self.config_json and self.config_json['msFitThresholdPerc']:
            params['thresholdperc'] = self.config_json['msFitThresholdPerc']
        # --thresholdpval
        if 'msFitThresholdPval' in self.config_json and self.config_json['msFitThresholdPval']:
            params['thresholdpval'] = self.config_json['msFitThresholdPval']
        # --maxraresigs
        if 'msFitMaxRareSigs' in self.config_json and self.config_json['msFitMaxRareSigs']:
            params['maxraresigs'] = self.config_json['msFitMaxRareSigs']
        # --nboot
        if 'msFitNBoot' in self.config_json and self.config_json['msFitNBoot']:
            params['bootstrap'] = ''
            params['nboot'] = self.config_json['msFitNBoot']
        # --signaturesfile
        if 'msFitSignaturesFile' in self.config_json and self.config_json['msFitSignaturesFile']:
            signatures_file_fpath = os.path.join(self.resource_dir, self.config_json['msFitSignaturesFile'])
            params['signaturesfile'] = signatures_file_fpath
        # --raresignaturesfile
        if 'msFitRareSignaturesFile' in self.config_json and self.config_json['msFitRareSignaturesFile']:
            rare_signatures_file_fpath = os.path.join(self.resource_dir, self.config_json['msFitRareSignaturesFile'])
            params['raresignaturesfile'] = rare_signatures_file_fpath

        # Adding all params to CMD
        for param in params:
            if len(str(params[param])) > 0:
                cmd += ' --{}={}'.format(param, params[param])
            else:
                cmd += ' --{}'.format(param)

        return cmd, params

    def create_signature_fitting(self):
        """Create the mutational signature fitting"""
        LOGGER.info('Creating the signature fitting')

        # Runnning fitting
        cmd, params = self.get_fitting_command()
        execute_bash_command(cmd)

        # Getting fitting scores
        if SIGNATURE_COEFFS_FILENAME not in list_dir_files(self.output_dir):
            msg = 'Signature fitting coefficients file "{}" could not be generated'.format(SIGNATURE_COEFFS_FILENAME)
            raise ValueError(msg)
        fitting_coeffs_fhand = open(os.path.join(self.output_dir, SIGNATURE_COEFFS_FILENAME), 'r')
        fitting_coeffs_keys = fitting_coeffs_fhand.readline().strip().split()
        fitting_coeffs_values = fitting_coeffs_fhand.readline().strip().split()[1:]  # Skip first value (sample name)
        fitting_scores = ([{'signatureId': k, 'value': float(v)}
                           for k, v in zip(fitting_coeffs_keys, fitting_coeffs_values)])

        # Getting signature source
        if self.config_json['msFitSigVersion'].startswith('RefSig'):
            source = 'RefSig'
        elif self.config_json['msFitSigVersion'].startswith('COSMIC'):
            source = 'COSMIC'
        else:
            source = None

        # Getting output files
        encoded_base64_files = []
        for f in list_dir_files(self.output_dir):
            if (f.endswith('.pdf') and f != 'catalogues.pdf') or f.startswith('fitData'):
                encoded_base64_files.append(convert_to_base64(os.path.join(self.output_dir, f)))
        for d in list_dir_dirs(self.output_dir):
            if d.startswith('selectedSolution'):
                for f in list_dir_files(os.path.join(self.output_dir, d)):
                    if f.endswith('.pdf'):
                        encoded_base64_files.append(convert_to_base64(os.path.join(self.output_dir, d, f)))

        # Getting fitting results
        fitting_results = {
            'method': self.config_json['msFitMethod'],
            'signatureSource': source,
            'signatureVersion': self.config_json['msFitSigVersion'],
            'scores': fitting_scores,
            'files': encoded_base64_files,
            'params': params
        }

        # Creating results
        if 'results.json' in list_dir_files(self.output_dir):
            # Getting signature counts
            results_fhand = open(os.path.join(self.output_dir, 'results.json'), 'r')
            results_json = json.loads(results_fhand.read())
            results_fhand.close()
            # Adding fitting results
            results_json['fitting'] = fitting_results
            generate_results_json(results=results_json, outdir_path=self.output_dir)
        else:
            generate_results_json(results=fitting_results, outdir_path=self.output_dir)

    def run(self):
        # Creating mutational signature catalogue
        if self.ms_type == 'SNV':
            LOGGER.info('Running mutational signature analysis for SNV')
            if os.path.basename(self.snv_genome_context_fpath) not in list_dir_files(self.resource_dir):
                self.create_snv_genome_context_file()
            self.create_snv_signature_catalogue()
            self.create_signature_fitting()
        elif self.ms_type == 'SV':
            LOGGER.info('Running mutational signature analysis for SV')
            self.create_sv_signature_catalogue()
            self.create_signature_fitting()
        else:
            msg = 'Mutational signature for type "{}" not implemented'.format(self.ms_type)
            raise ValueError(msg)
