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
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;

import java.util.HashMap;
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
        JCommander catalogSubCommands = jCommander.getCommands().get("variant");
        catalogSubCommands.addCommand("index", variantCommandOptions.indexVariantCommandOptions);
        catalogSubCommands.addCommand("query", variantCommandOptions.queryVariantCommandOptions);
        catalogSubCommands.addCommand("ibs", variantCommandOptions.ibsVariantCommandOptions);

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
        final QueryVariantCommandOptions queryVariantCommandOptions;
        final IbsVariantCommandOptions ibsVariantCommandOptions;
        final DeleteVariantCommandOptions deleteVariantCommandOptions;

        AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        public VariantCommandOptions() {
            this.indexVariantCommandOptions = new IndexVariantCommandOptions();
            this.queryVariantCommandOptions = new QueryVariantCommandOptions();
            this.ibsVariantCommandOptions = new IbsVariantCommandOptions();
            this.deleteVariantCommandOptions = new DeleteVariantCommandOptions();
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
//
//        @Parameter(names = {"-d", "--database"}, description = "DataBase name to load the data, eg. opencga_catalog")
//        public String database;
//
//        @Parameter(names = {"-H", "--host"}, description = "DataBase host and port, eg. localhost:27017")
//        public String host;
//
//        @Parameter(names = {"-p", "--password"}, description = "Admin password", password = true, arity = 0)
//        public String password;
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



    /*
     *  CATALOG SUB-COMMANDS
     */

    @Parameters(commandNames = {"index"}, commandDescription = "Index variants file")
    public class IndexVariantCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AnalysisCommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

//
//        @Parameter(names = {"-i", "--input"}, description = "File to index in the selected backend", required = true, variableArity = true)
//        public List<String> input;

//        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved (optional)", arity = 1, required = false)
//        public String outdir;

//        @Parameter(names = {"--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
//        public String fileId;

        @Parameter(names = {"--transform"}, description = "If present it only runs the transform stage, no load is executed")
        boolean transform = false;

        @Parameter(names = {"--load"}, description = "If present only the load stage is executed, transformation is skipped")
        boolean load = false;

//        @Parameter(names = {"--overwrite"}, description = "Reset the database if exists before installing")
//        public boolean overwrite;

//        @Parameter(names = {"--study-id"}, description = "Unque ID for the study", arity = 1)
//        public long studyId;

        @Parameter(names = {"--job-id"}, description = "Job id", hidden = true,required = false, arity = 1)
        public String jobId = null;

        @Parameter(names = {"--file-id"}, description = "Unique ID for the file", required = false, arity = 1)
        public String fileId = VariantStorageManager.Options.FILE_ID.defaultValue().toString();

        @Parameter(names = {"-o", "--outdir"}, description = "Unique ID for the file", required = false, arity = 1)
        public String outdirId = null;

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

        @Parameter(names = {"--ps", "--protein-substitution"}, description = "", required = false, arity = 1)
        public String proteinSubstitution;

        @Parameter(names = {"--gwas"}, description = "", required = false, arity = 1)
        public String gwas;

        @Parameter(names = {"--cosmic"}, description = "", required = false, arity = 1)
        public String cosmic;

        @Parameter(names = {"--clinvar"}, description = "", required = false, arity = 1)
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

        @Parameter(names = {"--count"}, description = "Count results. Do not return elements.", required = false, arity = 0)
        public boolean count;

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
}
