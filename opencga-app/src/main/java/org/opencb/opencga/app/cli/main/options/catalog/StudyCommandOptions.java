package org.opencb.opencga.app.cli.main.options.catalog;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;

import java.util.Map;

/**
 * Created by pfurio on 13/06/16.
 */
@Parameters(commandNames = {"studies"}, commandDescription = "Study commands")
public class StudyCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public FilesCommandOptions filesCommandOptions;
    public ScanFilesCommandOptions scanFilesCommandOptions;
    public StatusCommandOptions statusCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public SummaryCommandOptions summaryCommandOptions;
    public AlignmentsCommandOptions alignmentsCommandOptions;
    public JobsCommandOptions jobsCommandOptions;
    public SamplesCommandOptions samplesCommandOptions;
    public VariantsCommandOptions variantsCommandOptions;
    public HelpCommandOptions helpCommandOptions;

    public GroupsCommandOptions groupsCommandOptions;
    public GroupsCreateCommandOptions groupsCreateCommandOptions;
    public GroupsDeleteCommandOptions groupsDeleteCommandOptions;
    public GroupsInfoCommandOptions groupsInfoCommandOptions;
    public GroupsUpdateCommandOptions groupsUpdateCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsCreateCommandOptionsTemplate aclsCreateCommandOptions;
    public AclCommandOptions.AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    public AclCommandOptions.AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    public AclCommandOptions.AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    private AclCommandOptions aclCommandOptions;

    public StudyCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.filesCommandOptions = new FilesCommandOptions();
        this.scanFilesCommandOptions = new ScanFilesCommandOptions();
        this.statusCommandOptions = new StatusCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.summaryCommandOptions = new SummaryCommandOptions();
        this.alignmentsCommandOptions = new AlignmentsCommandOptions();
        this.jobsCommandOptions = new JobsCommandOptions();
        this.samplesCommandOptions = new SamplesCommandOptions();
        this.variantsCommandOptions = new VariantsCommandOptions();
        this.helpCommandOptions = new HelpCommandOptions();

        this.groupsCommandOptions = new GroupsCommandOptions();
        this.groupsCreateCommandOptions = new GroupsCreateCommandOptions();
        this.groupsDeleteCommandOptions = new GroupsDeleteCommandOptions();
        this.groupsInfoCommandOptions = new GroupsInfoCommandOptions();
        this.groupsUpdateCommandOptions = new GroupsUpdateCommandOptions();

        aclCommandOptions = new AclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsCreateCommandOptions = aclCommandOptions.getAclsCreateCommandOptionsTemplate();
        this.aclsMemberDeleteCommandOptions = aclCommandOptions.getAclsMemberDeleteCommandOptions();
        this.aclsMemberInfoCommandOptions = aclCommandOptions.getAclsMemberInfoCommandOptions();
        this.aclsMemberUpdateCommandOptions = aclCommandOptions.getAclsMemberUpdateCommandOptions();
    }

    public abstract class BaseStudyCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Study identifier", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create new study")
    public class CreateCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--project-id"}, description = "Project identifier", required = true, arity = 1)
        public String projectId;

        @Parameter(names = {"-n", "--name"}, description = "Study name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-a", "--alias"}, description = "Study alias", required = true, arity = 1)
        public String alias;

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", arity = 1)
        public String type = "CASE_CONTROL";

        @Parameter(names = {"-d", "--description"}, description = "Description", arity = 1)
        public String description;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get study information")
    public class InfoCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search studies")
    public class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--ids"}, description = "Comma separated list of study ids", arity = 1)
        public String id;

        @Parameter(names = {"--project-id"}, description = "Project Id.", arity = 1)
        public String projectId;

        @Parameter(names = {"--name"}, description = "Study name.", arity = 1)
        public String name;

        @Parameter(names = {"--alias"}, description = "Study alias.", arity = 1)
        public String alias;

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", arity = 1)
        public String type;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", arity = 1)
        public String creationDate;

        @Parameter(names = {"--status"}, description = "Status.", arity = 1)
        public String status;

        @Parameter(names = {"--attributes"}, description = "Attributes.", arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "Numerical attributes.", arity = 1)
        public String nattributes;

        @Parameter(names = {"--battributes"}, description = "Boolean attributes.", arity = 0)
        public String battributes;

        @Parameter(names = {"--count"}, description = "Total number of results. Default = false", arity = 0)
        public boolean count;

    }

    @Parameters(commandNames = {"scan-files"}, commandDescription = "Scans the study folder to find untracked or missing files")
    public class ScanFilesCommandOptions extends BaseStudyCommand {

    }


    @Parameters(commandNames = {"files"}, commandDescription = "Fetch files from a study")
    public class FilesCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--file-id"}, description = "File id", arity = 1)
        public String fileId;

        @Parameter(names = {"--name"}, description = "Name", arity = 1)
        public String name;

        @Parameter(names = {"--path"}, description = "Path", arity = 1)
        public String path;

        @Parameter(names = {"-t", "--file-type"}, description = "Comma separated Type values. For existing Types see files/help", arity = 1)
        public String type = "FILE";

        @Parameter(names = {"-b", "--bioformat"}, description = "Comma separated Bioformat values. For existing Bioformats see files/help",
                arity = 1)
        public String bioformat;

        @Parameter(names = {"--format"}, description = "Comma separated Format values. For existing Formats see files/help", arity = 1)
        public String format;

        @Parameter(names = {"--status"}, description = "Status", arity = 1)
        public String status;

        @Parameter(names = {"--directory"}, description = "Directory", arity = 1)
        public String directory;

        @Parameter(names = {"--owner-id"}, description = "owner Id", arity = 1)
        public String ownerId;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", arity = 1)
        public String creationDate;

        @Parameter(names = {"--modification-date"}, description = "Modification Date.", arity = 1)
        public String modificationDate;

        @Parameter(names = {"--description"}, description = "Description", arity = 1)
        public String description;

        @Parameter(names = {"--disk-usage"}, description = "DiskUsage", arity = 1)
        public String diskUsage;

        @Parameter(names = {"--sample-ids"}, description = "Comma separated sampleIds", arity = 1)
        public String sampleIds;

        @Parameter(names = {"--job-id"}, description = "Job Id", arity = 1)
        public String jobId;

        @Parameter(names = {"--attributes"}, description = "Attributes.", arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "Numerical attributes.", arity = 1)
        public String nattributes;

        @Parameter(names = {"-e", "--external"}, description = "Whether to fetch external linked files", arity = 0)
        public boolean external;

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--count"}, description = "Total number of results.", arity = 0)
        public boolean count;
    }


    @Parameters(commandNames = {"status"}, commandDescription = "Scans the study folder to find untracked or missing files")
    public class StatusCommandOptions extends BaseStudyCommand {

    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update the attributes of a study")
    public class UpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"-n", "--name"}, description = "Study name", arity = 1)
        public String name;

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", arity = 1)
        public String type;

        @Parameter(names = {"-d", "--description"}, description = "Organization", arity = 1)
        public String description;

        @Parameter(names = {"--stats"}, description = "Stats", arity = 1)
        public String stats;

        @Parameter(names = {"--attributes"}, description = "Attributes", arity = 1)
        public String attributes;

    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete a study [PENDING]")
    public class DeleteCommandOptions extends BaseStudyCommand {

    }

    @Parameters(commandNames = {"summary"}, commandDescription = "Summary with the general stats of a study")
    public class SummaryCommandOptions extends BaseStudyCommand {

    }

    @Parameters(commandNames = {"alignments"}, commandDescription = "Fetch alignments")
    public class AlignmentsCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--sample-id"}, description = "Sample id.", required = true, arity = 1)
        public String sampleId;

        @Parameter(names = {"--file-id"}, description = "File id.", required = true, arity = 1)
        public String fileId;

        @Parameter(names = {"--region"}, description = "Region.", required = true, arity = 1)
        public String region;

        @Parameter(names = {"--view-as-pairs"}, description = "View_as_pairs. Default = false", arity = 0)
        public boolean view_as_pairs;

        @Parameter(names = {"--include-coverage"}, description = "Include_coverage. Default = true", arity = 0)
        public boolean include_coverage = true;

        @Parameter(names = {"--process-differences"}, description = "Process differences. Default = true", arity = 0)
        public boolean process_differences = true;

        @Parameter(names = {"--histogram"}, description = "Histogram. Default = false", arity = 0)
        public boolean histogram;

        @Parameter(names = {"--interval"}, description = "Interval. Default = 2000", arity = 1)
        public Integer interval = 2000;

        @Parameter(names = {"--count"}, description = "Total number of results. Default = false", arity = 0)
        public boolean count;
    }

    @Parameters(commandNames = {"jobs"}, commandDescription = "Study jobs information")
    public class JobsCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--name"}, description = "Job name", arity = 1)
        public String name;

        @Parameter(names = {"--tool-name"}, description = "Tool name", arity = 1)
        public String toolName;

        @Parameter(names = {"--status"}, description = "Job status", arity = 1)
        public String status;

        @Parameter(names = {"--owner-id"}, description = "User that created the job", arity = 1)
        public String ownerId;

        /*@Parameter(names = {"--date"}, description = "Creation date of the job", arity = 1)
        public String date;*/

        @Parameter(names = {"--input-files"}, description = "Comma separated list of input file ids", arity = 1)
        public String inputFiles;

        @Parameter(names = {"--output-files"}, description = "Comma separated list of output file ids", arity = 1)
        public String outputFiles;

        @Parameter(names = {"--count"}, description = "Total number of results.", arity = 0)
        public boolean count;

    }

    @Parameters(commandNames = {"samples"}, commandDescription = "Study samples information")
    public class SamplesCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--name"}, description = "Sample name", arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Source of the sample", arity = 1)
        public String source;

        /*@Parameter(names = {"--description"}, description = "Sample description", arity = 1)
        public String description;*/

        @Parameter(names = {"--individual-id"}, description = "Individual id", arity = 1)
        public String individualId;

        @Parameter(names = {"--annotation-set-name"}, description = "AnnotationSetName", arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--variable-set-id"}, description = "Variable set id", arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation", arity = 1)
        public String annotation;

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--count"}, description = "Total number of results.", arity = 0)
        public boolean count;
    }

    @Parameters(commandNames = {"variants"}, commandDescription = "Study variants information")
    public class VariantsCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--variant-ids"}, description = "List of variant ids", arity = 1)
        public String ids;

        @Parameter(names = {"--region"}, description = "List of regions: {chr}:{start}-{end}", arity = 1)
        public String region;

        @Parameter(names = {"--chromosome"}, description = "List of chromosomes", arity = 1)
        public String chromosome;

        @Parameter(names = {"--gene"}, description = "List of genes", arity = 1)
        public String gene;

        @Parameter(names = {"--type"}, description = "Variant type: [SNV, MNV, INDEL, SV, CNV]", arity = 1)
        public String type;

        @Parameter(names = {"--reference"}, description = "Reference allele", arity = 1)
        public String reference;

        @Parameter(names = {"--alternate"}, description = "Main alternate allele", arity = 1)
        public String alternate;

        @Parameter(names = {"--returned-studies"}, description = "List of studies to be returned", arity = 1)
        public String returnedStudies;

        @Parameter(names = {"--returned-samples"}, description = "List of samples to be returned", arity = 1)
        public String returnedSamples;

        @Parameter(names = {"--returned-files"}, description = "List of files to be returned.", arity = 1)
        public String returnedFiles;

        @Parameter(names = {"--files"}, description = "Variants in specific files", arity = 1)
        public String files;

        @Parameter(names = {"--maf"}, description = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String maf;

        @Parameter(names = {"--mgf"}, description = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String mgf;

        @Parameter(names = {"--missing-alleles"}, description = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String missingAlleles;

        @Parameter(names = {"--missing-genotypes"}, description = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String missingGenotypes;

        @Parameter(names = {"--annotation-exists"}, description = "Specify if the variant annotation must exists.",
                arity = 0)
        public boolean annotationExists;

        @Parameter(names = {"--genotype"}, description = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}"
                + "(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1", arity = 1)
        public String genotype;

        @Parameter(names = {"--annot-ct"}, description = "Consequence type SO term list. e.g. SO:0000045,SO:0000046",
                arity = 1)
        public String annot_ct;

        @Parameter(names = {"--annot-xref"}, description = "XRef", arity = 1)
        public String annot_xref;

        @Parameter(names = {"--annot-biotype"}, description = "Biotype", arity = 1)
        public String annot_biotype;

        @Parameter(names = {"--polyphen"}, description = "Polyphen, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description}"
                + " e.g. <=0.9 , =benign", arity = 1)
        public String polyphen;

        @Parameter(names = {"--sift"}, description = "Sift, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} "
                + "e.g. >0.1 , ~=tolerant", arity = 1)
        public String sift;

        @Parameter(names = {"--conservation"}, description = "VConservation score: {conservation_score}[<|>|<=|>=]{number} "
                + "e.g. phastCons>0.5,phylop<0.1,gerp>0.1", arity = 1)
        public String conservation;

        @Parameter(names = {"--annot-population-maf"}, description = "Population minor allele frequency: "
                + "{study}:{population}[<|>|<=|>=]{number}", arity = 1)
        public String annotPopulationMaf;

        @Parameter(names = {"--alternate-frequency"}, description = "Alternate Population Frequency: "
                + "{study}:{population}[<|>|<=|>=]{number}", arity = 1)
        public String alternate_frequency;

        @Parameter(names = {"--reference-frequency"}, description = "Reference Population Frequency:"
                + " {study}:{population}[<|>|<=|>=]{number}", arity = 1)
        public String reference_frequency;

        @Parameter(names = {"--annot-transcription-flags"}, description = "List of transcript annotation flags. "
                + "e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno", arity = 1)
        public String transcriptionFlags;

        @Parameter(names = {"--annot-gene-trait-id"}, description = "List of gene trait association id. e.g. \"umls:C0007222\" , "
                + "\"OMIM:269600\"", arity = 1)
        public String geneTraitId;


        @Parameter(names = {"--annot-gene-trait-name"}, description = "List of gene trait association names. "
                + "e.g. \"Cardiovascular Diseases\"", arity = 1)
        public String geneTraitName;

        @Parameter(names = {"--annot-hpo"}, description = "List of HPO terms. e.g. \"HP:0000545\"", arity = 1)
        public String hpo;

        @Parameter(names = {"--annot-go"}, description = "List of GO (Genome Ontology) terms. e.g. \"GO:0002020\"", arity = 1)
        public String go;

        @Parameter(names = {"--annot-expression"}, description = "List of tissues of interest. e.g. \"tongue\"", arity = 1)
        public String expression;

        @Parameter(names = {"--annot-protein-keywords"}, description = "List of protein variant annotation keywords",
                arity = 1)
        public String proteinKeyword;

        @Parameter(names = {"--annot-drug"}, description = "List of drug names", arity = 1)
        public String drug;

        @Parameter(names = {"--annot-functional-score"}, description = "Functional score: {functional_score}[<|>|<=|>=]{number} "
                + "e.g. cadd_scaled>5.2 , cadd_raw<=0.3", arity = 1)
        public String functionalScore;

        @Parameter(names = {"--unknown-genotype"}, description = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]",
                arity = 1)
        public String unknownGenotype;

        @Parameter(names = {"--samples-metadata"}, description = "Returns the samples metadata group by studyId, instead of the variants",
                arity = 0)
        public boolean samplesMetadata;

        @Parameter(names = {"--sort"}, description = "Sort the results", arity = 0)
        public boolean sort;

        @Parameter(names = {"--group-by"}, description = "Group variants by: [ct, gene, ensemblGene]", arity = 1)
        public String groupBy;

        @Parameter(names = {"--count"}, description = "Count results", arity = 0)
        public boolean count;

        @Parameter(names = {"--histogram"}, description = "Calculate histogram. Requires one region.", arity = 0)
        public boolean histogram;

        @Parameter(names = {"--interval"}, description = "Histogram interval size. Default:2000", arity = 1)
        public String interval;

        @Parameter(names = {"--merge"}, description = "Merge results", arity = 1)
        public String merge;

    }

    @Parameters(commandNames = {"groups"}, commandDescription = "Return the groups present in the studies")
    public class GroupsCommandOptions extends BaseStudyCommand {

    }

    @Parameters(commandNames = {"help"}, commandDescription = "Help [PENDING]")
    public class HelpCommandOptions {
        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;
    }

    @Parameters(commandNames = {"groups-create"}, commandDescription = "Create a group")
    public class GroupsCreateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--group-id"}, description = "Group id, group id corresponds to the name of the group", required = true, arity = 1)
        public String groupId;

        @Parameter(names = {"--users"}, description = "Comma separated list of members that will form the group",
                required = true, arity = 1)
        public String users;
    }

    @Parameters(commandNames = {"groups-delete"}, commandDescription = "Delete group")
    public class GroupsDeleteCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--group-id"}, description = "Group id, group id corresponds to the name of the group ", required = true, arity = 1)
        public String groupId;

    }

    @Parameters(commandNames = {"groups-info"}, commandDescription = "Return the group")
    public class GroupsInfoCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--group-id"}, description = "Group id, group id corresponds to the name of the group", required = true, arity = 1)
        public String groupId;
    }

    @Parameters(commandNames = {"groups-update"}, commandDescription = "Updates the members of the group")
    public class GroupsUpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--group-id"}, description = "Group id, group id corresponds to the name of the group", required = true, arity = 1)
        public String groupId;

        @Parameter(names = {"--add-users"}, description = "Comma separated list of users that will be added to the group", arity = 1)
        public String addUsers;

        @Parameter(names = {"--set-users"}, description = "Comma separated list of users that will be added to the group", arity = 1)
        public String setUsers;

        @Parameter(names = {"--remove-users"}, description = "Comma separated list of users that will be added to the group", arity = 1)
        public String removeUsers;
    }
}
