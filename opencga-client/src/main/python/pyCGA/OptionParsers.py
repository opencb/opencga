#!/usr/bin/python
import argparse
__author__ = 'antonior'

class option_parser_login:

    def __init__(self):
        self.parser = argparse.ArgumentParser(description='This tool provide the interface to log in and logout to the system', add_help=True)
        self.subparsers = self.parser.add_subparsers(help='Tool', dest='tool')
        self.login_subparsers = self.subparsers.add_parser("login", help="Use this command to log in the system and get a Session Id")
        self.login_subparsers.add_argument('--host', metavar='OpenCGA Host', required=True, help='Optional')
        self.login_subparsers.add_argument('--instance', metavar='instance',  default="opencga", required=False, help='Optional')
        self.login_subparsers.add_argument('--user', metavar='User', required=True, help='Required. User name')
        self.login_subparsers.add_argument('--pwd', metavar='Password', required=False, help='Required. User password')
        self.login_subparsers.add_argument('--debug', default=False, action='store_true', help='Optional. Store log in /home/user/openCGA.log')
        self.logout_subparsers = self.subparsers.add_parser("logout", help="Use this command to log out, using your current Session Id")
        self.logout_subparsers.add_argument('--user', metavar='User', required=True, help='Required. User name')


class option_parser:

    def __init__(self):

        self.parser = argparse.ArgumentParser(description='This program work with openCGA using WebServices', add_help=False)
        # These are the general sections: Users, Variables, Files...
        self.subparsers = self.parser.add_subparsers(help='Tool', dest='tool')

        """
        User
        """

        self.parser_user = self.subparsers.add_parser('users', help='Use this program to log-in, log-out, add user(only admins) and delete user(only admin).')
        # These are the specific method for each general tool(login, logout, create....)
        self.user_subparsers = self.parser_user.add_subparsers(help="method", dest="method")
        self.create_parser = self.user_subparsers.add_parser('create', help="Use this command to create a new user, only administrators are allowed to perform this action")
        self.create_parser.add_argument('--user', metavar='user', required=True, help='Required. User name')
        self.create_parser.add_argument('--name', metavar='name', required=True, help='Required. User full name')
        self.create_parser.add_argument('--email', metavar='email', required=True, help='Required. Uses email')
        self.create_parser.add_argument('--pwd', metavar='password', required=True, help='Required. password')
        self.create_parser.add_argument('--org', metavar='organization', required=True, help='Required. Organization')
        self.delete_parser = self.user_subparsers.add_parser('delete', help="Use this command to delete a existing user, only administrators are allowed to perform this action")
        self.delete_parser.add_argument('--userId', metavar='UserId', required=True, help='Required. User name')
        self.acl_parser = self.user_subparsers.add_parser('acl', help="Use this command to check the permission of an existing user")
        self.acl_parser.add_argument('--userId', metavar='UserId', required=True, help='Required. User name')

        """
        Variables sets
        """

        self.parser_variables = self.subparsers.add_parser('variables', help='Commands to perform actions around variable sets')
        self.variables_subparsers = self.parser_variables.add_subparsers(help="method", dest="method")
        self.create_parser = self.variables_subparsers.add_parser('create', help="Create new annotation Set")
        self.create_parser.add_argument('--name', metavar='NameOfVariableSet', required=True, help='Required. Name of the new variable set')
        self.create_parser.add_argument('--studyId', metavar='studyID', required=True, help='Required. Study Id were the variable sets will be stored')
        self.create_parser.add_argument('--json', metavar='json', required=True, help='Required. Json file with the variableSet')
        self.create_parser.add_argument('--format', metavar='format', default="OPENCGA", choices=("AVRO", "OPENCGA"), help='Default: OPENCGA')

        """
        Files
        """

        self.parser_file = self.subparsers.add_parser('files', help='methods for files', description='methods for files')
        # These are the specific method for each general tool(login, logout, create....)
        self.file_subparsers = self.parser_file.add_subparsers(help="method", dest="method")
        self.index_parser = self.file_subparsers.add_parser('index', help="index a bam or vcf file")
        self.index_parser.add_argument('--fileId', metavar='file id', required=True, help='Required')
        self.index_parser.add_argument('--outdir', metavar='output directory', required=False, help='Required')
        self.index_parser.add_argument('--annotate', default=False, action='store_true', help='Variants will be annotated')
        self.index_parser.add_argument('--studyId', metavar='StudyId', required=True, help='Required')
        self.info_parser = self.file_subparsers.add_parser('info', help="reetrieve info on a file given its id")
        self.info_parser.add_argument('--fileId', metavar='file id', required=True, help='Required')
        self.info_parser.add_argument('--subset', metavar='file id', required=True, help='Required')
        self.info_parser.add_argument('--field', metavar='file id', required=True, help='Required')
        self.query_parser = self.file_subparsers.add_parser('query_stats', help="retrieve info on a file given its id")
        self.query_parser.add_argument('--studyId', metavar='study id', required=True, help='Required')
        self.query_parser.add_argument('--fileType', metavar='file type (ALIGNMENT o VARIANTS)', required=True, help='Required')
        self.query_parser.add_argument('--query', metavar='query e.g BAM_HEADER_MACHINE.MACHINE', required=True, help='Required')

        self.search_by_sample_parser = self.file_subparsers.add_parser('search_by_sample', help="retrieve the ids and the path of the files")
        self.search_by_sample_parser.add_argument('--studyId', metavar='study id', required=True, help='Required')
        self.search_by_sample_parser.add_argument('--fileType', metavar='file type (ALIGNMENT or VARIANTS)', required=True, help='Required')
        self.search_by_sample_parser.add_argument('--sampleName', metavar='Sample Name', nargs='*', required=True, help='Required')
        self.search_by_sample_parser.add_argument('--path', metavar='path', required=False, default="~^", help='Optional. Path under the search will be done. i,e: ~^by_date/*')

        self.link_file_parser = self.file_subparsers.add_parser('link_file', help="link a single file")
        self.link_file_parser.add_argument('--studyId', metavar='study id', required=True, help='Required')
        self.link_file_parser.add_argument('--uri', metavar='full path to file', required=True, help='Required')
        self.link_file_parser.add_argument('--sampleId', metavar='catalog sample id for file', required=False, help='Not Required')
        self.link_file_parser.add_argument('--path', metavar='Path in catalog', required=True, help='Required')

        """
        Individuals
        """

        self.parser_individual = self.subparsers.add_parser('individuals', help='methods for individuals', description='methods for individuals')
        # These are the specific method for each general tool(login, logout, create....)
        self.individual_subparsers = self.parser_individual.add_subparsers(help="method", dest="method")
        self.individuals_create_parser = self.individual_subparsers.add_parser('create', help="create an individual and assign samples to that individual")
        self.individuals_create_parser.add_argument('--name', metavar='name', required=True, help='Required')
        self.individuals_create_parser.add_argument('--gender', metavar='name', required=True, help='required - MALE FEMALE or UNKNOWN')
        self.individuals_create_parser.add_argument('--family', metavar='family', default="",required=False, help='Not Required')
        self.individuals_create_parser.add_argument('--fatherId', metavar='fatherid', default="",required=False, help='Not Required')
        self.individuals_create_parser.add_argument('--motherId', metavar='motherid', default="",required=False, help='Not Required')
        self.individuals_create_parser.add_argument('--studyId', metavar='StudyId', required=True, help='Required')
        self.individuals_create_parser.add_argument('--wellIds', metavar='wellids', required=True, help='comma separated list of wellids')

        self.individuals_search_parser = self.individual_subparsers.add_parser('search', help="search individuals given the individual name (participant id")
        self.individuals_search_parser.add_argument('--name', metavar='name', required=True, help='Required')
        self.individuals_search_parser.add_argument('--studyId', metavar='StudyId', required=True, help='Required')

        self.individuals_search_by_annotation_parser = self.individual_subparsers.add_parser('search_by_annotation', help="")
        self.individuals_search_by_annotation_parser.add_argument('--variableSetName', metavar='variableSetName', required=True, help='Required')
        self.individuals_search_by_annotation_parser.add_argument('--studyId', metavar='StudyId', required=True, help='Required')
        self.individuals_search_by_annotation_parser.add_argument('--queries', metavar='queries', required=True, nargs='*', help='Required')
        self.individuals_search_by_annotation_parser.add_argument('--outfile', metavar='outfile', required=True, help='Required')

        self.individuals_annotate_parser = self.individual_subparsers.add_parser('annotate', help="")
        self.individuals_annotate_parser.add_argument('--variableSetName', metavar='variableSetName', required=True, help='Required')
        self.individuals_annotate_parser.add_argument('--individualName', metavar='individualName', required=True, help='Required')
        self.individuals_annotate_parser.add_argument('--annotationSetName', metavar='annotationSetName', required=True, help='Required')
        self.individuals_annotate_parser.add_argument('--jsonFile', metavar='jsonFile', required=True, help='Required')
        self.individuals_annotate_parser.add_argument('--studyId', metavar='StudyId', required=True, help='Required')

        """
        Samples
        """
        self.parser_sample = self.subparsers.add_parser('samples', help='methods for samples', description='methods for samples')
        # These are the specific method for each general tool(login, logout, create....)
        self.sample_subparsers = self.parser_sample.add_subparsers(help="method", dest="method")
        self.samples_search_by_name_parser = self.sample_subparsers.add_parser('search_by_name', help="")
        self.samples_search_by_name_parser.add_argument('--name', metavar='URI', required=True, help='Required')
        self.samples_search_by_name_parser.add_argument('--studyId', metavar='StudyId', required=True, help='Required')

        self.samples_search_by_annotation_parser = self.sample_subparsers.add_parser('search_by_annotation', help="")
        self.samples_search_by_annotation_parser.add_argument('--variableSetName', metavar='variableSetName', required=True, help='Required')
        self.samples_search_by_annotation_parser.add_argument('--studyId', metavar='StudyId', required=True, help='Required')
        self.samples_search_by_annotation_parser.add_argument('--queries', metavar='queries', required=True, nargs='*', help='Required')

        self.samples_annotate_parser = self.sample_subparsers.add_parser('annotate', help="")
        self.samples_annotate_parser.add_argument('--variableSetName', metavar='variableSetName', required=True, help='Required')
        self.samples_annotate_parser.add_argument('--sampleName', metavar='sampleName', required=True, help='Required')
        self.samples_annotate_parser.add_argument('--annotationSetName', metavar='annotationSetName', required=True, help='Required')
        self.samples_annotate_parser.add_argument('--jsonFile', metavar='jsonFile', required=True, help='Required')
        self.samples_annotate_parser.add_argument('--studyId', metavar='StudyId', required=True, help='Required')


class option_parser_id_converter:

    def __init__(self):

        self.parser = argparse.ArgumentParser(description='This program can be used to get the apropiate Sample/individualID from openCGA', add_help=True, formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=80, width=150))
        self.parser.add_argument('--studyID', metavar='study Id', required=True, help='Required. The id of the study, this can be query usgin pyCGA')
        self.parser.add_argument('--inputIDType', metavar='Input Id Type', required=False, default="CatalogSampleId", choices=['CatalogSampleId', 'CatalogIndividualId', "SampleName", "IndividualName"], help='Optional. This is the type of id you are providing. You can choose from: ' + ", ".join(['CatalogSampleId', 'CatalogIndividualId', "SampleName", "IndividualName"]))
        self.parser.add_argument('--outputIDType', metavar='Output Id Type', required=False, default="IndividualName", choices=['CatalogSampleId', 'CatalogIndividualId', "SampleName", "IndividualName"], help='Optional. This is the type of id you are getting.  You can choose from: ' + ", ".join(['CatalogSampleId', 'CatalogIndividualId', "SampleName", "IndividualName"]))
        self.group = self.parser.add_mutually_exclusive_group()
        self.group.add_argument('--IdBatch', metavar='Id batch', required=False, help='Optional. File with ids, one id per line in the first column.')
        self.group.add_argument('--Id', metavar='Id', required=False, nargs='*', help='Optional. List of Ids space separated, e.g, --Id 31145 36457 346578')




class option_parser_fetcher:

    def __init__(self):

        self.parser = argparse.ArgumentParser(description='This program can be used to fetch variant easily from openCGA', add_help=True, formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=80, width=150))
        self.parser.add_argument('--studyID', metavar='study Id', required=True, help='Required. The id of the study, this can be query usgin pyCGA')
        self.parser.add_argument('--ids', metavar='ids', help='Optional. Select by variant ids (i,e dbSNP ids). Use comma to separate the ids')
        self.parser.add_argument('--region', metavar='region', help='Optional. Select by region (i,e chr:start-end). Use comma to separate the region')
        self.parser.add_argument('--chromosome', metavar='chromosome', help='Optional. Select by chromosome. Use comma to separate the region')
        self.parser.add_argument('--gene', metavar='gene', help='Optional. Select by gene name (It can be ENSMBL Ids or HGNCs symbols. Use comma to separate the gene names')
        self.parser.add_argument('--type', metavar='type', help='Optional. Select by type of variant  [SNV, MNV, INDEL, SV, CNV]. This filter should not be used alone due to the huge amount of results could be retrieved')
        self.parser.add_argument('--reference', metavar='reference', help='Optional. Select by reference base(s)')
        self.parser.add_argument('--alternate', metavar='alternate', help='Optional. Select by alternate base(s)')
        self.parser.add_argument('--files', metavar='files', help='Optional. Select by files, only the variants found in these file will be selected. Please note these are the id files in the DB, use pyCGA to get the id files. Use comma to separate the file ids')
        self.parser.add_argument('--maf', metavar='maf', help='Optional. Select by minor allele frequency. In this filter only the samples in the db are consider, not any external population. Syntax: [<|>|<=|>=]{number}, (e.g, >=0.05)')
        self.parser.add_argument('--mgf', metavar='mgf', help='Optional. Select by minor genotype frequency. In this filter only the samples in the db are consider, not any external population. Syntax: [<|>|<=|>=]{number}, (e.g, >=0.1)')
        self.parser.add_argument('--missingAlleles', metavar='missingAlleles', help='Optional. Select by number of missing alleles in the whole sample set. In this filter only the samples in the db are consider, not any external population. Syntax: [<|>|<=|>=]{number}, (e.g, >=0.05)')
        self.parser.add_argument('--missingGenotypes', metavar='missingGenotypes', help='Optional. Select by number of missing genotypes in the whole sample set. In this filter only the samples in the db are consider, not any external population. Syntax: [<|>|<=|>=]{number}, (e.g, >=0.05)')
        self.parser.add_argument('--annotationExists', default=False, action='store_true', help='Optional. Select only the annotated variants')
        self.parser.add_argument('--annotationDoesNotExist', default=False, action='store_false', help='Optional. Select only the variants without annotation')
        self.parser.add_argument('--genotype', metavar='genotype', help='Optional. Select by sample genotype. Samples names must be specified as they are stored the db. Please, find more information in the documentation, Syntax: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)* (e.g. HG0097:0/0;HG0098:0/1,1/1)')
        self.parser.add_argument('--consequence_type', metavar='consequence_type', help='Optional. Select by consequence type. Consequence type SO term list. Use comma to separate the SO terms. Please, find the information of SO terms supported in the documentation. (e.g. SO:0000045,SO:0000046')
        self.parser.add_argument('--xref', metavar='xref', help='Optional. Select by XRef, this is a field used to map ids from different dbs. Please, find the information of dbs supported in the documentation. ')
        self.parser.add_argument('--biotype', metavar='biotype', help='Optional. Select by Biotype. Consequence type SO term list. Use comma to separate the biotypes. Please, find the information of biotype terms supported in the documentation. (e.g. protein_coding,retained_intron. This filter should not be used alone due to the huge amount of results could be retrieved')
        self.parser.add_argument('--polyphen', metavar='polyphen', help='Optional. Select by polyphen, polyphen score ranges from [0-1]. Syntax: [<|>|<=|>=]{number}, (e.g, >=0.9). This filter is slow if it is not used along others')
        self.parser.add_argument('--sift', metavar='sift', help='Optional. Select by sift, sift score ranges from [0-1]. Syntax: [<|>|<=|>=]{number}, (e.g, >=0.9). This filter is slow if it is not used along others')
        self.parser.add_argument('--conservation', metavar='conservation', help='Optional. Select by conservation sources. Please read the documentation to find the sources available. Use comma to separate the conservation sources. Syntax: sourceName[<|>|<=|>=]number (e.g. phastCons>0.5,phylop<0.1). . This filter is slow if it is not used along others')
        self.parser.add_argument('--alternate_frequency', metavar='alternate_frequency', help='Optional. Select by frequency of the alternate allele in one population. Please read the documentation to find the population available. Use comma to separate the population. Syntax: populationName[<|>|<=|>=]number (e.g. 1000g_CEU>0.5,1000g_AFR<0.1)')
        self.parser.add_argument('--reference_frequency', metavar='reference_frequency', help='Optional. Select by frequency of the reference allele in one population. Please read the documentation to find the population available. Use comma to separate the population. Syntax: populationName[<|>|<=|>=]number (e.g. 1000g_CEU>0.5,1000g_AFR<0.1)')

        self.parser.add_argument('--limit', metavar='reference_frequency', help='Optional. limit (number of results)')
        self.parser.add_argument('--skip', metavar='reference_frequency', help='Optional. skip (number of results)')
        self.parser.add_argument('--sort', default=False, action='store_true', help='Optional. Sort the output by chromosome coordinates')
        self.parser.add_argument('--group_by', metavar='group_by', help='Optional. Group the output by ')
        self.parser.add_argument('--exclude', metavar='exclude', help='Optional. Exclude some parts of the output')

        self.parser.add_argument('--unknownGenotype', metavar='unknownGenotype', default="./.", help='Optional. Returned genotype for unknown genotypes. ')
        self.parser.add_argument('--returnedSamples', metavar='returnedSamples', help='Optional. Only the specified samples will be returned. The samples names in the db, if you have doubts about this, please read the documentation. Use comma to separate the sample names')
        self.parser.add_argument('--returnedFiles', metavar='returnedFiles', help='Optional. Only the information from the specified files will be returned.Please note these are the id files in the DB, use pyCGA to get the id files. Use comma to separate the files id')

        self.parser.add_argument('--batchSize', metavar='batchSize',  default=5000, help='Optional. This parameter control the size of the batches of variants per query, This number is proportional to the memory used and inversely proportional to the time. This value can not be greater than 5000.')

        self.parser.add_argument('--outputType', metavar='output',  default="json", choices=['json', 'VCF', "AVRO-GA4GH", "AVRO-OPENCGA"], help='Optional. This parameter control the size of the batches of variants per query, This number is proportional to the memory used and inversely proportional to the time. This value can not be greater than 5000.')
        self.parser.add_argument('--outputFile', metavar='outputFile', required=True,  help='Optional. Output file with the result')

def get_options_fetcher():
    op = option_parser_fetcher()
    parser = op.parser
    return parser

def get_options_login():
    op = option_parser_login()
    parser = op.parser
    return parser

def get_options_id_converter():
    op = option_parser_id_converter()
    parser = op.parser
    return parser

def get_options_pycga():
    op = option_parser()
    parser = op.parser
    return parser