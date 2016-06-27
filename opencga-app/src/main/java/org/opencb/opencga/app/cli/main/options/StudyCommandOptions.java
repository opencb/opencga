package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;
import org.opencb.opencga.catalog.models.Study;

import java.util.List;

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
    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

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
    }

    public abstract class BaseStudyCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study identifier", required = true, arity = 1)
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

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", required = false, arity = 1)
        public String type;

        @Parameter(names = {"-d", "--description"}, description = "Organization", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--status"}, description = "Status.",
                required = false, arity = 1)
        public String status;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get study information")
    public class InfoCommandOptions extends BaseStudyCommand {
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search studies")
    public class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Id.", required = false, arity = 1)
        public String id;

        @Parameter(names = {"--project-id"}, description = "Project Id.", required = false, arity = 1)
        public String projectId;

        @Parameter(names = {"--name"}, description = "Study name.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--alias"}, description = "Study alias.", required = false, arity = 1)
        public String alias;

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", required = false, arity = 1)
        public String type;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", required = false, arity = 1)
        public String creationDate;

        @Parameter(names = {"--status"}, description = "Status.", required = false, arity = 1)
        public String status;

        @Parameter(names = {"--attributes"}, description = "Attributes.", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "Numerical attributes.", required = false, arity = 1)
        public String nattributes;

        @Parameter(names = {"--battributes"}, description = "Boolean attributes.", required = false, arity = 0)
        public boolean battributes;

        @Parameter(names = {"--groups"}, description = "Groups.", required = false, arity = 1)
        public String groups;

        @Parameter(names = {"--groups-users"}, description = "Groups users.", required = false, arity = 1)
        public String groupsUsers;
    }

    @Parameters(commandNames = {"scan-files"},
            commandDescription = "Scans the study folder to find untracked or missing files")
    public class ScanFilesCommandOptions extends BaseStudyCommand {  }


    @Parameters(commandNames = {"files"}, commandDescription = "Fetch files from a study")
    public class FilesCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"-t", "--file-type"}, description = "Filter type of files, eg. file,directory", required = false, arity = 1)
        public String type = "FILE";

        @Parameter(names = {"-b", "--bioformat"}, description = "Filter by bioformat of files, eg. VARIANT,IDLIST", arity = 1)
        public String bioformat;

        @Parameter(names = {"-s", "--size"}, description = "Filter by size of files, eg. <100000", required = false, arity = 1)
        public String size;

        @Parameter(names = {"-e", "--external"}, description = "Whether to fetch external linked files", required = false, arity = 0)
        public boolean external;
    }


    @Parameters(commandNames = {"status"}, commandDescription = "Scans the study folder to find untracked or missing files")
    public class StatusCommandOptions extends BaseStudyCommand {
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Study modify")
    public class UpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"-n", "--name"}, description = "Study name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", required = false, arity = 1)
        public String type;

        @Parameter(names = {"-d", "--description"}, description = "Organization", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--status"}, description = "Status.",
                required = false, arity = 1)
        public String status;


    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete a study [PENDING]")
    public class DeleteCommandOptions extends BaseStudyCommand {
    }

    @Parameters(commandNames = {"summary"}, commandDescription = "Summary with the general stats of a study")
    public class SummaryCommandOptions extends BaseStudyCommand {
    }

    @Parameters(commandNames = {"alignments"}, commandDescription = "Study alignments information")
    public class AlignmentsCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--sample-id"}, description = "Sample id.", required = true, arity = 1)
        public String sampleId;

        @Parameter(names = {"--file-id"}, description = "File id.", required = true, arity = 1)
        public String fileId;

        @Parameter(names = {"--region"}, description = "Region.", required = true, arity = 1)
        public String region;

        @Parameter(names = {"--view-as-pairs"}, description = "View_as_pairs.", required = false, arity = 0)
        public boolean view_as_pairs;

        @Parameter(names = {"--include-coverage"}, description = "Include_coverage.", required = false, arity = 0)
        public boolean include_coverage;

        @Parameter(names = {"--process-differences"}, description = "Process-differences.", required = false, arity = 0)
        public boolean process_differences;

//        @Parameter(names = {"--view-as-pairs"}, description = "View_as_pairs.", required = false, arity = 0)
//        public boolean histogram;

        @Parameter(names = {"--interval"}, description = "Interval.", required = false, arity = 1)
        public Integer interval;
    }

    @Parameters(commandNames = {"jobs"}, commandDescription = "Study jobs information")
    public class JobsCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--name"}, description = "Job name", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--tool-id"}, description = "Tool Id PENDING", required = false, arity = 1)
        public String toolId;

        @Parameter(names = {"--execution"}, description = "Execution", required = false, arity = 1)
        public String execution;

        @Parameter(names = {"--description"}, description = "Job description", required = false, arity = 1)
        public String description;
    }

    @Parameters(commandNames = {"samples"}, commandDescription = "Study samples information")
    public class SamplesCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--name"}, description = "Job name", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Source Id", required = false, arity = 1)
        public String source;

        @Parameter(names = {"--description"}, description = "Sample description", required = false, arity = 1)
        public String description;
    }

    @Parameters(commandNames = {"variants"}, commandDescription = "Study samples information")
    public class VariantsCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--ids"}, description = "List of variant ids", required = false, arity = 1)
        public String ids;

        @Parameter(names = {"--region"}, description = "List of regions: {chr}:{start}-{end}", required = false, arity = 1)
        public String region;

        @Parameter(names = {"--chromosome"}, description = "List of chromosomes", required = false, arity = 1)
        public String chromosome;

        @Parameter(names = {"--gene"}, description = "List of genes", required = false, arity = 1)
        public String gene;

        @Parameter(names = {"--type"}, description = "Variant type: [SNV, MNV, INDEL, SV, CNV]", required = false, arity = 1)
        public String type;

        @Parameter(names = {"--reference"}, description = "Reference allele", required = false, arity = 1)
        public String reference;

        @Parameter(names = {"--alternate"}, description = "Main alternate allele", required = false, arity = 1)
        public String alternate;

        @Parameter(names = {"--returned-studies"}, description = "List of studies to be returned", required = false, arity = 1)
        public String returnedStudies;

        @Parameter(names = {"--returned-samples"}, description = "List of samples to be returned", required = false, arity = 1)
        public String returnedSamples;

        @Parameter(names = {"--returned-files"}, description = "List of files to be returned.", required = false, arity = 1)
        public String returnedFiles;

        @Parameter(names = {"--files"}, description = "Variants in specific files", required = false, arity = 1)
        public String files;

        @Parameter(names = {"--maf"}, description = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}",
                required = false, arity = 1)
        public String maf;

        @Parameter(names = {"--mgf"}, description = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}",
                required = false, arity = 1)
        public String mgf;

        @Parameter(names = {"--missing-alleles"}, description = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}",
                required = false, arity = 1)
        public String missingAlleles;

        @Parameter(names = {"--missing-genotypes"}, description = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}",
                required = false, arity = 1)
        public String missingGenotypes;

        @Parameter(names = {"--annotation-exists"}, description = "Specify if the variant annotation must exists.",
                required = false, arity = 1)
        public String annotationExists;

        @Parameter(names = {"--genotype"}, description = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}"
                + "(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1", required = false, arity = 1)
        public String genotype;

        @Parameter(names = {"--annot-ct"}, description = "Consequence type SO term list. e.g. SO:0000045,SO:0000046",
                required = false, arity = 1)
        public String annot_ct;

        @Parameter(names = {"--annot-xref"}, description = "XRef", required = false, arity = 1)
        public String annot_xref;

        @Parameter(names = {"--annot-biotype"}, description = "Biotype", required = false, arity = 1)
        public String annot_biotype;

        @Parameter(names = {"--polyphen"}, description = "Polyphen, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description}"
                + " e.g. <=0.9 , =benign", required = false, arity = 1)
        public String polyphen;

        @Parameter(names = {"--sift"}, description = "Sift, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} "
                + "e.g. >0.1 , ~=tolerant", required = false, arity = 1)
        public String sift;

        @Parameter(names = {"--conservation"}, description = "VConservation score: {conservation_score}[<|>|<=|>=]{number} "
                + "e.g. phastCons>0.5,phylop<0.1,gerp>0.1", required = false, arity = 1)
        public String conservation;

        @Parameter(names = {"--annot-population-maf"}, description = "Population minor allele frequency: "
                + "{study}:{population}[<|>|<=|>=]{number}", required = false, arity = 1)
        public String annotPopulationMaf;

        @Parameter(names = {"--alternate-frequency"}, description = "Alternate Population Frequency: "
                + "{study}:{population}[<|>|<=|>=]{number}", required = false, arity = 1)
        public String alternate_frequency;

        @Parameter(names = {"--reference-frequency"}, description = "Reference Population Frequency:"
                + " {study}:{population}[<|>|<=|>=]{number}", required = false, arity = 1)
        public String reference_frequency;

        @Parameter(names = {"--annot-transcription-flags"}, description = "List of transcript annotation flags. "
                + "e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno", required = false, arity = 1)
        public String transcriptionFlags;

        @Parameter(names = {"--annot-gene-trait-id"}, description = "List of gene trait association id. e.g. \"umls:C0007222\" , "
                + "\"OMIM:269600\"", required = false, arity = 1)
        public String geneTraitId;


        @Parameter(names = {"--annot-gene-trait-name"}, description = "List of gene trait association names. "
                + "e.g. \"Cardiovascular Diseases\"", required = false, arity = 1)
        public String geneTraitName;

        @Parameter(names = {"--annot-hpo"}, description = "List of HPO terms. e.g. \"HP:0000545\"", required = false, arity = 1)
        public String hpo;

        @Parameter(names = {"--annot-protein-keywords"}, description = "List of protein variant annotation keywords",
                required = false, arity = 1)
        public String proteinKeyword;

        @Parameter(names = {"--annot-drug"}, description = "List of drug names", required = false, arity = 1)
        public String drug;

        @Parameter(names = {"--annot-functional-score"}, description = "Functional score: {functional_score}[<|>|<=|>=]{number} "
                + "e.g. cadd_scaled>5.2 , cadd_raw<=0.3", required = false, arity = 1)
        public String functionalScore;

        @Parameter(names = {"--unknown-genotype"}, description = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]",
                required = false, arity = 1)
        public String unknownGenotype;

        @Parameter(names = {"--samples-metadata"}, description = "Returns the samples metadata group by studyId, instead of the variants",
                required = false, arity = 1)
        public String samplesMetadata;

        @Parameter(names = {"--sort"}, description = "Sort the results", required = false, arity = 1)
        public String sort;

        @Parameter(names = {"--group-by"}, description = "Group variants by: [ct, gene, ensemblGene]", required = false, arity = 1)
        public String groupBy;

        @Parameter(names = {"--count"}, description = "Count results", required = false, arity = 1)
        public String count;

        @Parameter(names = {"--histogram"}, description = "Histogram interval size, default:2000", required = false, arity = 1)
        public String histogram;

        @Parameter(names = {"--interval"}, description = "Variants in specific files", required = false, arity = 1)
        public String interval;

        @Parameter(names = {"--merge"}, description = "Merge results", required = false, arity = 1)
        public String merge;
    }

    @Parameters(commandNames = {"assignRole"}, commandDescription = "Study samples information")
    public class AssignRoleCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--role"}, description = "Job name", required = false, arity = 1)
        public String role;

        @Parameter(names = {"--members"}, description = "Source Id", required = false, arity = 1)
        public String members;

        @Parameter(names = {"--override"}, description = "Sample description", required = false, arity = 0)
        public boolean override;
    }


}
