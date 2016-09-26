/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.app.cli.analysis;

import com.beust.jcommander.*;
import com.beust.jcommander.converters.CommaParameterSplitter;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 02/03/15.
 */
public class AnalysisCliOptionsParser {

    private final JCommander jCommander;

    private final GeneralCliOptions.GeneralOptions generalOptions;
    private final AnalysisCommonCommandOptions commonCommandOptions;

    private ExpressionCommandOptions expressionCommandOptions;
    private FunctionalCommandOptions functionalCommandOptions;
    private VariantCommandOptions variantCommandOptions;
    private ToolsCommandOptions toolsCommandOptions;
    private AlignmentCommandOptions alignmentCommandOptions;


    public AnalysisCliOptionsParser() {
        generalOptions = new GeneralCliOptions.GeneralOptions();

        jCommander = new JCommander(generalOptions);
        jCommander.setProgramName("opencga-analysis.sh");

        commonCommandOptions = new AnalysisCommonCommandOptions();

        expressionCommandOptions = new ExpressionCommandOptions();
        jCommander.addCommand("expression", expressionCommandOptions);
        JCommander auditSubCommands = jCommander.getCommands().get("expression");
        auditSubCommands.addCommand("diff", expressionCommandOptions.diffExpressionCommandOptions);
        auditSubCommands.addCommand("clustering", expressionCommandOptions.clusteringExpressionCommandOptions);

        functionalCommandOptions = new FunctionalCommandOptions();
        jCommander.addCommand("functional", functionalCommandOptions);
        JCommander usersSubCommands = jCommander.getCommands().get("functional");
        usersSubCommands.addCommand("fatigo", functionalCommandOptions.fatigoFunctionalCommandOptions);
        usersSubCommands.addCommand("gene-set", functionalCommandOptions.genesetFunctionalCommandOptions);

        variantCommandOptions = new VariantCommandOptions();
        jCommander.addCommand("variant", variantCommandOptions);
        JCommander variantSubCommands = jCommander.getCommands().get("variant");
        variantSubCommands.addCommand("index", variantCommandOptions.indexVariantCommandOptions);
        variantSubCommands.addCommand("stats", variantCommandOptions.statsVariantCommandOptions);
        variantSubCommands.addCommand("annotate", variantCommandOptions.annotateVariantCommandOptions);
        variantSubCommands.addCommand("query", variantCommandOptions.queryVariantCommandOptions);
        variantSubCommands.addCommand("export-frequencies", variantCommandOptions.exportVariantStatsCommandOptions);
        variantSubCommands.addCommand("ibs", variantCommandOptions.ibsVariantCommandOptions);

        alignmentCommandOptions = new AlignmentCommandOptions();
        jCommander.addCommand("alignment", alignmentCommandOptions);
        JCommander alignmentSubCommands = jCommander.getCommands().get("alignment");
        alignmentSubCommands.addCommand("index", alignmentCommandOptions.indexAlignmentCommandOptions);
        alignmentSubCommands.addCommand("query", alignmentCommandOptions.queryAlignmentCommandOptions);
//        alignmentSubCommands.addCommand("stats", alignmentCommandOptions.statsVariantCommandOptions);
//        alignmentSubCommands.addCommand("annotate", alignmentCommandOptions.annotateVariantCommandOptions);

        toolsCommandOptions = new ToolsCommandOptions();
        jCommander.addCommand("tools", toolsCommandOptions);
        JCommander toolsSubCommands = jCommander.getCommands().get("tools");
        toolsSubCommands.addCommand("install", toolsCommandOptions.installToolCommandOptions);
        toolsSubCommands.addCommand("list", toolsCommandOptions.listToolCommandOptions);
        toolsSubCommands.addCommand("show", toolsCommandOptions.showToolCommandOptions);
    }

    public void parse(String[] args) throws ParameterException {
        jCommander.parse(args);
    }

    public String getCommand() {
        return (jCommander.getParsedCommand() != null) ? jCommander.getParsedCommand() : "";
    }

    public String getSubCommand() {
        String parsedCommand = jCommander.getParsedCommand();
        if (jCommander.getCommands().containsKey(parsedCommand)) {
            String subCommand = jCommander.getCommands().get(parsedCommand).getParsedCommand();
            return subCommand != null ? subCommand: "";
        } else {
            return null;
        }
    }

    public boolean isHelp() {
        String parsedCommand = jCommander.getParsedCommand();
        if (parsedCommand != null) {
            JCommander jCommander2 = jCommander.getCommands().get(parsedCommand);
            List<Object> objects = jCommander2.getObjects();
            if (!objects.isEmpty() && objects.get(0) instanceof GeneralCliOptions.CommonCommandOptions) {
                return ((GeneralCliOptions.CommonCommandOptions) objects.get(0)).help;
            }
        }
        return commonCommandOptions.help;
    }

    /**
     * This class contains all those parameters available for all 'commands'
     */
    public class CommandOptions {

        @Parameter(names = {"-h", "--help"},  description = "This parameter prints this help", help = true)
        public boolean help;

        public JCommander getSubCommand() {
            return jCommander.getCommands().get(getCommand()).getCommands().get(getSubCommand());
        }

        public String getParsedSubCommand() {
            String parsedCommand = jCommander.getParsedCommand();
            if (jCommander.getCommands().containsKey(parsedCommand)) {
                String subCommand = jCommander.getCommands().get(parsedCommand).getParsedCommand();
                return subCommand != null ? subCommand: "";
            } else {
                return "";
            }
        }
    }

    /**
     * This class contains all those common parameters available for all 'subcommands'
     */
    public class AnalysisCommonCommandOptions extends GeneralCliOptions.CommonCommandOptions {

        @Parameter(names = {"--sid", "--session-id"}, description = "Token session id", arity = 1)
        public String sessionId;

        @Parameter(names = {"-u", "--user"}, description = "User name", arity = 1)
        public String user;

        @Parameter(names = {"-p", "--password"}, description = "User password", password = true, arity = 0)
        public String password;

        @DynamicParameter(names = "-D", description = "Storage engine specific parameters go here comma separated, ie. -Dmongodb" +
                ".compression=snappy", hidden = false)
        public Map<String, String> params = new HashMap<>(); //Dynamic parameters must be initialized

    }


    /*
     * Expression CLI options
     */
    @Parameters(commandNames = {"expression"}, commandDescription = "Implement gene expression analysis tools")
    public class ExpressionCommandOptions extends CommandOptions {

        DiffExpressionCommandOptions diffExpressionCommandOptions;
        ClusteringExpressionCommandOptions clusteringExpressionCommandOptions;

        AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        public ExpressionCommandOptions() {
            this.diffExpressionCommandOptions = new DiffExpressionCommandOptions();
            this.clusteringExpressionCommandOptions = new ClusteringExpressionCommandOptions();
        }
    }


    /*
     * Functional CLI options
     */
    @Parameters(commandNames = {"functional"}, commandDescription = "Implement functional genomic analysis tools")
    public class FunctionalCommandOptions extends CommandOptions {

        FatigoFunctionalCommandOptions fatigoFunctionalCommandOptions;
        GenesetFunctionalCommandOptions genesetFunctionalCommandOptions;

        AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        public FunctionalCommandOptions() {
            this.fatigoFunctionalCommandOptions = new FatigoFunctionalCommandOptions();
            this.genesetFunctionalCommandOptions = new GenesetFunctionalCommandOptions();
        }
    }


    /*
     * Variant CLI options
     */
    @Parameters(commandNames = {"variant"}, commandDescription = "Implement several tools for the genomic variant analysis")
    public class VariantCommandOptions extends CommandOptions {

        final IndexVariantCommandOptions indexVariantCommandOptions;
        final StatsVariantCommandOptions statsVariantCommandOptions;
        final AnnotateVariantCommandOptions annotateVariantCommandOptions;
        final QueryVariantCommandOptions queryVariantCommandOptions;
        final ExportVariantStatsCommandOptions exportVariantStatsCommandOptions;
        final IbsVariantCommandOptions ibsVariantCommandOptions;
        final DeleteVariantCommandOptions deleteVariantCommandOptions;

        AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        public VariantCommandOptions() {
            this.indexVariantCommandOptions = new IndexVariantCommandOptions();
            this.statsVariantCommandOptions = new StatsVariantCommandOptions();
            this.annotateVariantCommandOptions = new AnnotateVariantCommandOptions();
            this.queryVariantCommandOptions = new QueryVariantCommandOptions();
            this.exportVariantStatsCommandOptions = new ExportVariantStatsCommandOptions();
            this.ibsVariantCommandOptions = new IbsVariantCommandOptions();
            this.deleteVariantCommandOptions = new DeleteVariantCommandOptions();
        }
    }


    /*
     * Alignment CLI options
     */
    @Parameters(commandNames = {"alignment"}, commandDescription = "Implement several tools for the genomic alignment analysis")
    public class AlignmentCommandOptions extends CommandOptions {

        final IndexAlignmentCommandOptions indexAlignmentCommandOptions;
        final QueryAlignmentCommandOptions queryAlignmentCommandOptions;
//        final StatsVariantCommandOptions statsVariantCommandOptions;
//        final AnnotateVariantCommandOptions annotateVariantCommandOptions;
//        final DeleteVariantCommandOptions deleteVariantCommandOptions;

        AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        public AlignmentCommandOptions() {
            this.indexAlignmentCommandOptions = new IndexAlignmentCommandOptions();
            this.queryAlignmentCommandOptions = new QueryAlignmentCommandOptions();
        }
    }


    /*
     * Tools CLI options
     */
    @Parameters(commandNames = {"tools"}, commandDescription = "Implements different tools for working with tools")
    public class ToolsCommandOptions extends CommandOptions {

        InstallToolCommandOptions installToolCommandOptions;
        ListToolCommandOptions listToolCommandOptions;
        ShowToolCommandOptions showToolCommandOptions;

        AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        public ToolsCommandOptions() {
            this.installToolCommandOptions = new InstallToolCommandOptions();
            this.listToolCommandOptions = new ListToolCommandOptions();
            this.showToolCommandOptions = new ShowToolCommandOptions();
        }
    }


    /**
     * Auxiliary class for Database connection.
     */
    class CatalogDatabaseCommandOptions {

    }



    /*
     * EXPRESSION SUB-COMMANDS
     */
    @Parameters(commandNames = {"diff"}, commandDescription = "Query audit data from Catalog database")
    public class DiffExpressionCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--filter"}, description = "Query filter for data")
        public String filter;
    }

    @Parameters(commandNames = {"clustering"}, commandDescription = "Print summary stats for an user")
    public class ClusteringExpressionCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;
    }



    /*
     * USER SUB-COMMANDS
     */

    @Parameters(commandNames = {"fatigo"}, commandDescription = "Create a new user in Catalog database and the workspace")
    public class FatigoFunctionalCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--user-id"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        public String userId;

    }

    @Parameters(commandNames = {"gene-set"}, commandDescription = "Delete the user Catalog database entry and the workspace")
    public class GenesetFunctionalCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--user-id"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        public String userId;

    }

    public class JobCommand {
        @Parameter(names = {"--queue"}, description = "Enqueue the job. Do not execute", required = false, arity = 0)
        public boolean queue = false;

        @Parameter(names = {"--job-id"}, description = "Job id", hidden = true,required = false, arity = 1)
        public String jobId = null;
    }


    /*
     *  Variant SUB-COMMANDS
     */

    @Parameters(commandNames = {"index"}, commandDescription = "Index variants file")
    public class IndexVariantCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        @ParametersDelegate
        public JobCommand job = new JobCommand();

//
//        @Parameter(names = {"-i", "--input"}, description = "File to index in the selected backend", required = true, variableArity = true)
//        public List<String> input;

//        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved (optional)", arity = 1, required = false)
//        public String outdir;

//        @Parameter(names = {"--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
//        public String fileId;

        @Parameter(names = {"--transform"}, description = "If present it only runs the transform stage, no load is executed")
        public boolean transform = false;

        @Parameter(names = {"--load"}, description = "If present only the load stage is executed, transformation is skipped")
        public boolean load = false;

//        @Parameter(names = {"--overwrite"}, description = "Reset the database if exists before installing")
//        public boolean overwrite;

//        @Parameter(names = {"--study-id"}, description = "Unque ID for the study", arity = 1)
//        public long studyId;

        @Parameter(names = {"--file-id"}, description = "CSV of file ids to be indexed", required = true, arity = 1)
        public String fileId = null;

        @Parameter(names = {"--transformed-files"}, description = "CSV of paths corresponding to the location of the transformed files.",
                required = false, arity = 1)
        public String transformedPaths = null;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory outside catalog boundaries.", required = true, arity = 1)
        public String outdir = null;

        @Parameter(names = {"--path"}, description = "Path within catalog boundaries where the results will be stored. If not present, "
                + "transformed files will not be registered in catalog.", required = false, arity = 1)
        public String catalogPath = null;

        @Parameter(names = {"--exclude-genotypes"}, description = "Index excluding the genotype information")
        public boolean excludeGenotype = false;

        @Parameter(names = {"--include-extra-fields"}, description = "Index including other genotype fields [CSV]")
        public String extraFields = "";

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF " +
                "file")
        public String aggregationMappingFile = null;

        @Parameter(names = {"--gvcf"}, description = "The input file is in gvcf format")
        public boolean gvcf;

        @Parameter(names = {"--bgzip"}, description = "[PENDING] The input file is in bgzip format")
        public boolean bgzip;

        @Parameter(names = {"--calculate-stats"}, description = "Calculate indexed variants statistics after the load step")
        public boolean calculateStats = false;

        @Parameter(names = {"--annotate"}, description = "Annotate indexed variants after the load step")
        public boolean annotate = false;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        public org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource annotator = null;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        public boolean overwriteAnnotations;

    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Create and load stats into a database.")
    public class StatsVariantCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        @ParametersDelegate
        public JobCommand job = new JobCommand();

//        @Parameter(names = {"--create"}, description = "Run only the creation of the stats to a file")
//        public boolean create = false;
//
//        @Parameter(names = {"--load"}, description = "Load the stats from an already existing FILE directly into the database. FILE is a "
//                + "prefix with structure <INPUT_FILENAME>.<TIME>")
//        public boolean load = false;

        @Parameter(names = {"--overwrite-stats"}, description = "[PENDING] Overwrite stats in variants already present")
        public boolean overwriteStats = false;

        @Parameter(names = {"--region"}, description = "Region to calculate.")
        public String region;

        @Parameter(names = {"--update-stats"}, description = "Calculate stats just for missing positions. "
                + "Assumes that existing stats are correct")
        public boolean updateStats = false;

        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true,
                arity = 1)
        public String studyId;

        @Parameter(names = {"-f", "--file-id"}, description = "Calculate stats only for the selected file", required = false, arity = 1)
        public String fileId;

        @Parameter(names = {"--cohort-ids"}, description = "Cohort Ids for the cohorts to be calculated.")
        String cohortIds;

        // FIXME: Hidden?
        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: database name", required = false, arity = 1)
        public String fileName;

//        @Parameter(names = {"--outdir-id"}, description = "Output directory", required = false, arity = 1)
//        public String outdirId;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory outside catalog boundaries.", required = true, arity = 1)
        public String outdir = null;

        @Parameter(names = {"--path"}, description = "Path within catalog boundaries where the results will be stored. If not present, "
                + "transformed files will not be registered in catalog.", required = false, arity = 1)
        public String catalogPath = null;

//        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", required = false, arity = 1)
//        public String outdir = ".";

        @Parameter(names = {"--aggregated"}, description = "Aggregated VCF File: basic or EVS (optional)", arity = 1)
        VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF file")
        public String aggregationMappingFile;


    }


    @Parameters(commandNames = {"annotate"}, commandDescription = "Create and load variant annotations into the database")
    public class AnnotateVariantCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        @ParametersDelegate
        public JobCommand job = new JobCommand();

        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true,
                arity = 1)
        public String studyId;

        @Parameter(names = {"-o", "--outdir-id"}, description = "Output directory", required = false, arity = 1)
        public String outdirId;

        @Parameter(names = {"--create"}, description = "Run only the creation of the annotations to a file (specified by --output-filename)")
        public boolean create = false;

        @Parameter(names = {"--load"}, description = "Run only the load of the annotations into the DB from FILE")
        public String load = null;

        @Parameter(names = {"--custom-name"}, description = "Provide a name to the custom annotation")
        public String customAnnotationKey = null;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        public org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource annotator;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        public boolean overwriteAnnotations = false;

        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: dbName", required = false, arity = 1)
        public String fileName;

        @Parameter(names = {"--species"}, description = "Species. Default hsapiens", required = false, arity = 1)
        public String species = "hsapiens";

        @Parameter(names = {"--assembly"}, description = "Assembly. Default GRc37", required = false, arity = 1)
        public String assembly = "GRc37";

        @Parameter(names = {"--filter-region"}, description = "Comma separated region filters", splitter = CommaParameterSplitter.class)
        public List<String> filterRegion;

        @Parameter(names = {"--filter-chromosome"}, description = "Comma separated chromosome filters", splitter = CommaParameterSplitter.class)
        public List<String> filterChromosome;

        @Parameter(names = {"--filter-gene"}, description = "Comma separated gene filters", splitter = CommaParameterSplitter.class)
        public String filterGene;

        @Parameter(names = {"--filter-annot-consequence-type"}, description = "Comma separated annotation consequence type filters",
                splitter = CommaParameterSplitter.class)
        public List filterAnnotConsequenceType = null; // TODO will receive CSV, only available when create annotations

    }

    @Parameters(commandNames = {"query"}, commandDescription = "Search over indexed variants")
    public class QueryVariantCommandOptions extends QueryCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--id"}, description = "CSV list of variant ids", required = false)
        public String id;

        @Parameter(names = {"--group-by"}, description = "Group by gene, ensemblGene or consequence_type", required = false)
        public String groupBy;

        @Parameter(names = {"--rank"}, description = "Rank variants by gene, ensemblGene or consequence_type", required = false)
        public String rank;

        @Parameter(names = {"-s", "--study"}, description = "A comma separated list of studies to be used as filter", required = false)
        public String study;

        @Parameter(names = {"--sample-genotype"}, description = "A comma separated list of samples from the SAME study, example: " +
                "NA0001:0/0,0/1;NA0002:0/1", required = false, arity = 1)
        public String sampleGenotype;

        @Deprecated
        @Parameter(names = {"-f", "--file"}, description = "A comma separated list of files to be used as filter", required = false, arity = 1)
        public String file;

        @Parameter(names = {"-t", "--type"}, description = "Whether the variant is a: SNV, INDEL or SV", required = false)
        public String type;


//        @Parameter(names = {"--include-annotations"}, description = "Add variant annotation to the INFO column", required = false,
// arity = 0)
//        public boolean includeAnnotations;

        @Parameter(names = {"--annotations"}, description = "Set variant annotation to return in the INFO column. " +
                "Accepted values include 'all', 'default' aor a comma-separated list such as 'gene,biotype,consequenceType'", required = false, arity = 1)
        public String annotations;

        @Parameter(names = {"--ct", "--consequence-type"}, description = "Consequence type SO term list. example: SO:0000045,SO:0000046",
                required = false, arity = 1)
        public String consequenceType;

        @Parameter(names = {"--biotype"}, description = "Biotype CSV", required = false, arity = 1)
        public String biotype;

        @Parameter(names = {"--pf", "--population-frequency"}, description = "Alternate Population Frequency: " +
                "{study}:{population}[<|>|<=|>=]{number}", required = false, arity = 1)
        public String populationFreqs;

        @Parameter(names = {"--pmaf", "--population-maf"}, description = "Population minor allele frequency: " +
                "{study}:{population}[<|>|<=|>=]{number}", required = false, arity = 1)
        public String populationMaf;

        @Parameter(names = {"--conservation"}, description = "Conservation score: {conservation_score}[<|>|<=|>=]{number} example: " +
                "phastCons>0.5,phylop<0.1", required = false, arity = 1)
        public String conservation;

        @Parameter(names = {"--transcript-flag"}, description = "List of transcript annotation flags. e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno", required = false, arity = 1)
        public String flags;

        @Parameter(names = {"--gene-trait-id"}, description = "List of gene trait association names. e.g. \"Cardiovascular Diseases\"", required = false, arity = 1)
        public String geneTraitId;

        @Parameter(names = {"--gene-trait-name"}, description = "List of gene trait association id. e.g. \"umls:C0007222\" , \"OMIM:269600\"", required = false, arity = 1)
        public String geneTraitName;

        @Parameter(names = {"--hpo"}, description = "List of HPO terms. e.g. \"HP:0000545\" , \"HP:0002812\"", required = false, arity = 1)
        public String hpo;

        @Parameter(names = {"--go"}, description = "List of GO (Genome Ontology) terms. e.g. \"GO:0002020\"", required = false, arity = 1)
        public String go;

        @Parameter(names = {"--expression"}, description = "List of tissues of interest. e.g. \"tongue\"", required = false, arity = 1)
        public String expression;

        @Parameter(names = {"--protein-keywords"}, description = "List of protein variant annotation keywords", required = false, arity = 1)
        public String proteinKeywords;

        @Parameter(names = {"--drug"}, description = "List of drug names", required = false, arity = 1)
        public String drugs;

        @Parameter(names = {"--ps", "--protein-substitution"}, description = "Protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. <=0.9,benign", required = false, arity = 1)
        public String proteinSubstitution;

        @Parameter(names = {"--gwas"}, description = "[PENDING]", required = false, arity = 1)
        public String gwas;

        @Parameter(names = {"--cosmic"}, description = "[PENDING]", required = false, arity = 1)
        public String cosmic;

        @Parameter(names = {"--clinvar"}, description = "[PENDING]", required = false, arity = 1)
        public String clinvar;


        @Deprecated
        @Parameter(names = {"--stats"}, description = " [CSV]", required = false)
        public String stats;

        @Parameter(names = {"--maf"}, description = "Take a <STUDY>:<COHORT> and filter by Minor Allele Frequency, example: 1000g:all>0.4", required = false)
        public String maf;

        @Parameter(names = {"--mgf"}, description = "Take a <STUDY>:<COHORT> and filter by Minor Genotype Frequency, example: " +
                "1000g:all<=0.4", required = false)
        public String mgf;

        @Parameter(names = {"--missing-allele"}, description = "Take a <STUDY>:<COHORT> and filter by number of missing alleles, example:" +
                " 1000g:all=5", required = false)
        public String missingAlleleCount;

        @Parameter(names = {"--missing-genotype"}, description = "Take a <STUDY>:<COHORT> and filter by number of missing genotypes, " +
                "example: 1000g:all!=0", required = false)
        public String missingGenotypeCount;


        @Parameter(names = {"--dominant"}, description = "[PENDING] Take a family in the form of: FATHER,MOTHER,CHILD and specifies if is" +
                " affected or not to filter by dominant segregation, example: 1000g:NA001:aff,1000g:NA002:unaff,1000g:NA003:aff",
                required = false)
        public String dominant;

        @Parameter(names = {"--recessive"}, description = "[PENDING] Take a family in the form of: FATHER,MOTHER,CHILD and specifies if " +
                "is affected or not to filter by recessive segregation, example: 1000g:NA001:aff,1000g:NA002:unaff,1000g:NA003:aff",
                required = false)
        public String recessive;

        @Parameter(names = {"--ch", "--compound-heterozygous"}, description = "[PENDING] Take a family in the form of: FATHER,MOTHER," +
                "CHILD and specifies if is affected or not to filter by compound heterozygous, example: 1000g:NA001:aff," +
                "1000g:NA002:unaff,1000g:NA003:aff", required = false)
        public String compoundHeterozygous;


        @Parameter(names = {"--return-study"}, description = "A comma separated list of studies to be returned", required = false)
        public String returnStudy;

        @Parameter(names = {"--return-sample"}, description = "A comma separated list of samples from the SAME study to be returned", required = false)
        public String returnSample;

        @Parameter(names = {"--unknown-genotype"}, description = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]", required = false)
        public String unknownGenotype = "./.";

        @Parameter(names = {"--of", "--output-format"}, description = "Output format: vcf, vcf.gz, json or json.gz", required = false, arity = 1)
        public String outputFormat = "vcf";

    }

    class QueryCommandOptions {

        @Parameter(names = {"-o", "--output"}, description = "Output file. [STDOUT]", required = false, arity = 1)
        public String output;

        @Parameter(names = {"-r", "--region"}, description = "CSV list of regions: {chr}[:{start}-{end}]. example: 2,3:1000000-2000000",
                required = false)
        public String region;

        @Parameter(names = {"--region-file"}, description = "GFF File with regions", required = false)
        public String regionFile;

        @Parameter(names = {"-g", "--gene"}, description = "CSV list of genes", required = false)
        public String gene;

        @Parameter(names = {"-i", "--include"}, description = "", required = false, arity = 1)
        public String include;

        @Parameter(names = {"-e", "--exclude"}, description = "", required = false, arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Skip some number of elements.", required = false, arity = 1)
        public int skip;

        @Parameter(names = {"--limit"}, description = "Limit the number of returned elements.", required = false, arity = 1)
        public int limit;

        @Parameter(names = {"--sort"}, description = "Sort the output variants.")
        public boolean sort;

        @Parameter(names = {"--count"}, description = "Count results. Do not return elements.", required = false, arity = 0)
        public boolean count;

    }

    @Parameters(commandNames = {"export-frequencies"}, commandDescription = "Export calculated variant stats and frequencies")
    public class ExportVariantStatsCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        @ParametersDelegate
        public QueryCommandOptions queryOptions = new QueryCommandOptions();

        @Parameter(names = {"--of", "--output-format"}, description = "Output format: vcf, vcf.gz, tsv, tsv.gz, cellbase, cellbase.gz, json or json.gz", required = false, arity = 1)
        public String outputFormat = "tsv";

        @Parameter(names = {"-s", "--study"}, description = "A comma separated list of studies to be returned", required = false)
        public String studies;

    }

    @Parameters(commandNames = {"ibs"}, commandDescription = "[PENDING] ")
    public class IbsVariantCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

    }

    @Parameters(commandNames = {"delete"}, commandDescription = "[PENDING] Delete an indexed file from the Database")
    public class DeleteVariantCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--file-id"}, description = "File to delete")
        public boolean reset;
    }


    /*
     *  ALIGNMENT SUB-COMMANDS
     */


    @Parameters(commandNames = {"index-alignments"}, commandDescription = "Index alignment file")
    public class IndexAlignmentCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        @ParametersDelegate
        public JobCommand job = new JobCommand();

        @Parameter(names = {"-i", "--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
        public String fileId;

        @Parameter(names = "--calculate-coverage", description = "Calculate coverage while indexing")
        public boolean calculateCoverage = true;

        @Parameter(names = "--mean-coverage", description = "Specify the chunk sizes to calculate average coverage. Only works if flag " +
                "\"--calculate-coverage\" is also given. Please specify chunksizes as CSV: --mean-coverage 200,400", required = false)
        public List<String> meanCoverage;

        @Parameter(names = {"-o", "--outdir-id"}, description = "Directory where output files will be saved (optional)", arity = 1, required = false)
        public String outdirId;

        @Parameter(names = {"--transform"}, description = "If present it only runs the transform stage, no load is executed")
        boolean transform = false;

        @Parameter(names = {"--load"}, description = "If present only the load stage is executed, transformation is skipped")
        boolean load = false;

    }

    @Parameters(commandNames = {"query"}, commandDescription = "Search over indexed alignments")
    public class QueryAlignmentCommandOptions extends QueryCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"-s", "--study"}, description = "A comma separated list of studies to be used as filter", required = false)
        public String study;

        @Parameter(names = {"--file-id"}, description = "File unique ID.", required = false, arity = 1)
        public String fileId;
//
//        @Parameter(names = {"--file-path"}, description = "", required = false, arity = 1)
//        public String filePath;

        @Parameter(names = {"--include-coverage"}, description = " [CSV]", required = false)
        public boolean coverage = false;

        @Parameter(names = {"-H", "--histogram"}, description = " ", required = false, arity = 1)
        public boolean histogram = false;

        @Parameter(names = {"--view-as-pairs"}, description = " ", required = false)
        public boolean asPairs;

        @Parameter(names = {"--process-differences"}, description = " ", required = false)
        public boolean processDifferences;

        @Parameter(names = {"-S", "--stats-filter"}, description = " [CSV]", required = false)
        public List<String> stats = new LinkedList<>();
    }


    /*
     *  Tools SUB-COMMANDS
     */


    @Parameters(commandNames = {"install"}, commandDescription = "Install and check a new tool")
    public class InstallToolCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"-i", "--input"}, description = "File with the new tool to be installed", required = true, arity = 1)
        public String study;

    }

    @Parameters(commandNames = {"list"}, commandDescription = "Print a summary list of all tools")
    public class ListToolCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--filter"}, description = "Some kind of filter", arity = 1)
        public String study;

    }

    @Parameters(commandNames = {"show"}, commandDescription = "Show a summary of the tool")
    public class ShowToolCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--tool-id"}, description = "Full name of the study where the file is classified", arity = 1)
        public String study;

    }



    public void printUsage() {
        String parsedCommand = getCommand();
        if (parsedCommand.isEmpty()) {
            System.err.println("");
            System.err.println("Program:     OpenCGA (OpenCB)");
            System.err.println("Version:     " + GitRepositoryState.get().getBuildVersion());
            System.err.println("Git commit:  " + GitRepositoryState.get().getCommitId());
            System.err.println("Description: Big Data platform for processing and analysing NGS data");
            System.err.println("");
            System.err.println("Usage:       opencga-analysis.sh [-h|--help] [--version] <command> [options]");
            System.err.println("");
            System.err.println("Commands:");
            printMainUsage();
            System.err.println("");
        } else {
            String parsedSubCommand = getSubCommand();
            if (parsedSubCommand.isEmpty()) {
                System.err.println("");
                System.err.println("Usage:   opencga-analysis.sh " + parsedCommand + " <subcommand> [options]");
                System.err.println("");
                System.err.println("Subcommands:");
                printCommands(jCommander.getCommands().get(parsedCommand));
                System.err.println("");
            } else {
                System.err.println("");
                System.err.println("Usage:   opencga-analysis.sh " + parsedCommand + " " + parsedSubCommand + " [options]");
                System.err.println("");
                System.err.println("Options:");
                CommandLineUtils.printCommandUsage(jCommander.getCommands().get(parsedCommand).getCommands().get(parsedSubCommand));
                System.err.println("");
            }
        }
    }

    private void printMainUsage() {
        for (String s : jCommander.getCommands().keySet()) {
            System.err.printf("%14s  %s\n", s, jCommander.getCommandDescription(s));
        }
    }

    private void printCommands(JCommander commander) {
        for (Map.Entry<String, JCommander> entry : commander.getCommands().entrySet()) {
            System.err.printf("%14s  %s\n", entry.getKey(), commander.getCommandDescription(entry.getKey()));
        }
    }


    public GeneralCliOptions.GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    public AnalysisCommonCommandOptions getCommonOptions() {
        return commonCommandOptions;
    }

    public VariantCommandOptions getVariantCommandOptions() {
        return variantCommandOptions;
    }

    public FunctionalCommandOptions getFunctionalCommandOptions() {
        return functionalCommandOptions;
    }

    public ExpressionCommandOptions getExpressionCommandOptions() {
        return expressionCommandOptions;
    }

    public ToolsCommandOptions getToolsCommandOptions() {
        return toolsCommandOptions;
    }

    public AlignmentCommandOptions getAlignmentCommandOptions() {
        return alignmentCommandOptions;
    }

}
