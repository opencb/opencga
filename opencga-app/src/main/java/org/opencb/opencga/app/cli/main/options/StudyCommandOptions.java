/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.catalog.models.GroupParams;
import org.opencb.opencga.catalog.models.MemberParams;
import org.opencb.opencga.catalog.models.acls.AclParams;

import static org.opencb.opencga.app.cli.GeneralCliOptions.*;

/**
 * Created by pfurio on 13/06/16.
 */
@Parameters(commandNames = {"studies"}, commandDescription = "Study commands")
public class StudyCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public FilesCommandOptions filesCommandOptions;
    public ScanFilesCommandOptions scanFilesCommandOptions;
    public ResyncFilesCommandOptions resyncFilesCommandOptions;
    public StatusCommandOptions statusCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public SummaryCommandOptions summaryCommandOptions;
    public JobsCommandOptions jobsCommandOptions;
    public SamplesCommandOptions samplesCommandOptions;
    public VariantsCommandOptions variantsCommandOptions;
    public HelpCommandOptions helpCommandOptions;

    public GroupsCommandOptions groupsCommandOptions;
    public GroupsCreateCommandOptions groupsCreateCommandOptions;
    public GroupsDeleteCommandOptions groupsDeleteCommandOptions;
    public GroupsUpdateCommandOptions groupsUpdateCommandOptions;
    public MemberGroupUpdateCommandOptions memberGroupUpdateCommandOptions;

    public AclsCommandOptions aclsCommandOptions;
    public AclsUpdateCommandOptions aclsUpdateCommandOptions;

    public JCommander jCommander;
    public CommonCommandOptions commonCommandOptions;
    public DataModelOptions commonDataModelOptions;
    public NumericOptions commonNumericOptions;

    public StudyCommandOptions(CommonCommandOptions commonCommandOptions, DataModelOptions dataModelOptions, NumericOptions numericOptions,
                               JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.filesCommandOptions = new FilesCommandOptions();
        this.scanFilesCommandOptions = new ScanFilesCommandOptions();
        this.resyncFilesCommandOptions = new ResyncFilesCommandOptions();
        this.statusCommandOptions = new StatusCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.summaryCommandOptions = new SummaryCommandOptions();
        this.jobsCommandOptions = new JobsCommandOptions();
        this.samplesCommandOptions = new SamplesCommandOptions();
        this.variantsCommandOptions = new VariantsCommandOptions();
        this.helpCommandOptions = new HelpCommandOptions();

        this.groupsCommandOptions = new GroupsCommandOptions();
        this.groupsCreateCommandOptions = new GroupsCreateCommandOptions();
        this.groupsDeleteCommandOptions = new GroupsDeleteCommandOptions();
        this.groupsUpdateCommandOptions = new GroupsUpdateCommandOptions();
        this.memberGroupUpdateCommandOptions = new MemberGroupUpdateCommandOptions();

        this.aclsCommandOptions = new AclsCommandOptions();
        this.aclsUpdateCommandOptions = new AclsUpdateCommandOptions();
    }

    public abstract class BaseStudyCommand extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create new study")
    public class CreateCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project"}, description = "Project identifier, this parameter is optional when only one project exist",
                arity = 1)
        public String project;

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

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search studies")
    public class SearchCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"-p", "--project"}, description = "Project id or alias", arity = 1)
        public String project;

        @Parameter(names = {"-n", "--name"}, description = "Study name.", arity = 1)
        public String name;

        @Parameter(names = {"-a", "--alias"}, description = "Study alias.", arity = 1)
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


    }

    @Parameters(commandNames = {"scan-files"}, commandDescription = "Scans the study folder to find untracked or missing files")
    public class ScanFilesCommandOptions extends BaseStudyCommand {

    }

    @Parameters(commandNames = {"resync-files"}, commandDescription = "Scans the study folder to find untracked or missing files and "
            + "updates the status")
    public class ResyncFilesCommandOptions extends BaseStudyCommand {

    }


    @Parameters(commandNames = {"files"}, commandDescription = "Fetch files from a study")
    public class FilesCommandOptions extends BaseStudyCommand {

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Deprecated
        @Parameter(names = {"--file"}, description = "[DEPRECATED] File id", arity = 1)
        public String file;

        @Parameter(names = {"-n", "--name"}, description = "Name", arity = 1)
        public String name;

        @Deprecated
        @Parameter(names = {"--path"}, description = "[DEPRECATED] Path", arity = 1)
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

        @Parameter(names = {"--creation-date"}, description = "Creation date.", arity = 1)
        public String creationDate;

        @Parameter(names = {"--modification-date"}, description = "Modification Date.", arity = 1)
        public String modificationDate;

        @Deprecated
        @Parameter(names = {"--description"}, description = "Description", arity = 1)
        public String description;

        @Parameter(names = {"--size"}, description = "Filter by size of the file", arity = 1)
        public String size;

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

    @Parameters(commandNames = {"delete"}, commandDescription = "[PENDING] Delete a study")
    public class DeleteCommandOptions extends BaseStudyCommand {

    }

    @Parameters(commandNames = {"summary"}, commandDescription = "Summary with the general stats of a study")
    public class SummaryCommandOptions extends BaseStudyCommand {

    }

    @Parameters(commandNames = {"jobs"}, commandDescription = "Study jobs information")
    public class JobsCommandOptions extends BaseStudyCommand {

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"-n", "--name"}, description = "Job name", arity = 1)
        public String name;

        @Parameter(names = {"--tool-name"}, description = "Tool name", arity = 1)
        public String toolName;

        @Parameter(names = {"--status"}, description = "Job status", arity = 1)
        public String status;

        @Parameter(names = {"--owner-id"}, description = "User that created the job", arity = 1)
        public String ownerId;

        @Parameter(names = {"--date"}, description = "Creation date of the job", arity = 1)
        public String date;

        @Deprecated
        @Parameter(names = {"--input-files"}, description = "[DEPRECATED] Comma separated list of input file ids", arity = 1)
        public String inputFiles;

        @Deprecated
        @Parameter(names = {"--output-files"}, description = "[DEPRECATED] Comma separated list of output file ids", arity = 1)
        public String outputFiles;

    }

    @Parameters(commandNames = {"samples"}, commandDescription = "Study samples information")
    public class SamplesCommandOptions extends BaseStudyCommand {

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"-n", "--name"}, description = "Sample name", arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Source of the sample", arity = 1)
        public String source;

        @Parameter(names = {"-d", "--description"}, description = "Sample description", arity = 1)
        public String description;

        @Parameter(names = {"--individual"}, description = "Individual id", arity = 1)
        public String individual;

        @Parameter(names = {"--annotation-set-name"}, description = "AnnotationSetName", arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--variable-set-id"}, description = "Variable set id", arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation", arity = 1)
        public String annotation;

    }

    @Deprecated
    @Parameters(commandNames = {"variants"}, commandDescription = "[DEPRECATED] Use analysis instead")
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

        @Parameter(names = {"--type"}, description = "Variant types: [SNV, MNV, INDEL, SV, CNV]", arity = 1)
        public VariantType type;

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

//        @Parameter(names = {"--annotation-exists"}, description = "Specify if the variant annotation must exists.",
//                arity = 0)
//        public boolean annotationExists;

        @Parameter(names = {"--genotype"}, description = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}"
                + "(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1", arity = 1)
        public String genotype;

        @Parameter(names = {"--annot-ct"},
                description = "Consequence type SO term list. e.g. missense_variant,stop_lost or SO:0001583,SO:0001578", arity = 1)
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

        @Parameter(names = {"--samples-metadata"}, description = "Returns the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.",
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

//        @Parameter(names = {"--merge"}, description = "Merge results", arity = 1)
//        public String merge;

    }

    @Parameters(commandNames = {"groups"}, commandDescription = "Return the groups present in the studies")
    public class GroupsCommandOptions extends BaseStudyCommand {
        @Parameter(names = {"--name"}, description = "Group name. If present, it will fetch only information of the group provided.",
                arity = 1)
        public String group;
    }

    @Parameters(commandNames = {"help"}, commandDescription = "Help [PENDING]")
    public class HelpCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

    }

    @Parameters(commandNames = {"groups-create"}, commandDescription = "Create a group")
    public class GroupsCreateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--name"}, description = "Group name.", required = true, arity = 1)
        public String groupId;

        @Parameter(names = {"--users"}, description = "Comma separated list of members that will form the group", arity = 1)
        public String users;

    }

    @Parameters(commandNames = {"groups-delete"}, commandDescription = "Delete group")
    public class GroupsDeleteCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--name"}, description = "Group name", required = true, arity = 1)
        public String groupId;

    }

    @Parameters(commandNames = {"groups-update"}, commandDescription = "Updates the members of the group")
    public class GroupsUpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--name"}, description = "Group name", required = true, arity = 1)
        public String groupId;

        @Parameter(names = {"--users"}, description = "Comma separated list of users", required = true, arity = 1)
        public String users;

        @Parameter(names = {"--action"}, description = "Action to be performed over users (ADD, SET, REMOVE)", required = true, arity = 1)
        public GroupParams.Action action;
    }

    @Parameters(commandNames = {"members-update"}, commandDescription = "Add/Remove users to access the study")
    public class MemberGroupUpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--users"}, description = "Comma separated list of users", required = true, arity = 1)
        public String users;

        @Parameter(names = {"--action"}, description = "Action to be performed over users (ADD, REMOVE)", required = true, arity = 1)
        public MemberParams.Action action;
    }

    @Parameters(commandNames = {"acl"}, commandDescription = "Return the acls set for the resource")
    public class AclsCommandOptions extends BaseStudyCommand {
        @Parameter(names = {"-m", "--member"}, description = "Member id  ('userId', '@groupId' or '*'). If provided, only returns "
                + "acls given to the member.", arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-update"}, commandDescription = "Update the permissions set for a member")
    public class AclsUpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"-m", "--member"}, description = "Member id  ('userId', '@groupId' or '*')", required = true, arity = 1)
        public String memberId;

        @Parameter(names = {"-p", "--permissions"}, description = "Comma separated list of accepted permissions for the resource",
                arity = 1)
        public String permissions;

        @Parameter(names = {"-a", "--action"}, description = "Comma separated list of accepted permissions for the resource", arity = 1)
        public AclParams.Action action = AclParams.Action.SET;

        @Parameter(names = {"--template"}, description = "Template of permissions to be used (admin, analyst or view_only)", arity = 1)
        public String template;
    }
}
