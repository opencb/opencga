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

package org.opencb.opencga.app.cli.analysis;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.CliOptionsParser;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.analysis.options.AlignmentCommandOptions;
import org.opencb.opencga.app.cli.analysis.options.InterpretationCommandOptions;
import org.opencb.opencga.app.cli.analysis.options.ToolsCommandOptions;
import org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.List;

import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.VariantScoreIndexCommandOptions.SCORE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.VariantScoreRemoveCommandOptions.SCORE_REMOVE_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.VariantSecondaryIndexCommandOptions.SECONDARY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.VariantSecondaryIndexRemoveCommandOptions.SECONDARY_INDEX_REMOVE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillGapsCommandOptions.FILL_GAPS_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillMissingCommandOptions.FILL_MISSING_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions.ANNOTATION_DELETE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions.ANNOTATION_METADATA_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions.ANNOTATION_QUERY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions.ANNOTATION_SAVE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantRemoveCommandOptions.VARIANT_REMOVE_COMMAND;

/**
 * Created by imedina on 02/03/15.
 */
public class AnalysisCliOptionsParser extends CliOptionsParser {

    private final GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    private final GeneralCliOptions.DataModelOptions dataModelOptions;
    private final GeneralCliOptions.NumericOptions numericOptions;

    private ExpressionCommandOptions expressionCommandOptions;
    private FunctionalCommandOptions functionalCommandOptions;
    private org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions variantCommandOptions;
//    private VariantCommandOptions variantCommandOptions;
    private ToolsCommandOptions toolsCommandOptions;
    private AlignmentCommandOptions alignmentCommandOptions;
    private InterpretationCommandOptions interpretationCommandOptions;


    public AnalysisCliOptionsParser() {
        jCommander.setProgramName("opencga-analysis.sh");

        commonCommandOptions = new GeneralCliOptions.CommonCommandOptions();
        dataModelOptions = new GeneralCliOptions.DataModelOptions();
        numericOptions = new GeneralCliOptions.NumericOptions();

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

        variantCommandOptions = new org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions(commonCommandOptions,
                dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("variant", variantCommandOptions);
        JCommander variantSubCommands = jCommander.getCommands().get("variant");
        variantSubCommands.addCommand("index", variantCommandOptions.indexVariantCommandOptions);
        variantSubCommands.addCommand(VARIANT_REMOVE_COMMAND, variantCommandOptions.variantRemoveCommandOptions);
        variantSubCommands.addCommand(SECONDARY_INDEX_COMMAND, variantCommandOptions.variantSecondaryIndexCommandOptions);
        variantSubCommands.addCommand(SECONDARY_INDEX_REMOVE_COMMAND, variantCommandOptions.variantSecondaryIndexRemoveCommandOptions);
        variantSubCommands.addCommand("stats", variantCommandOptions.statsVariantCommandOptions);
        variantSubCommands.addCommand(SCORE_INDEX_COMMAND, variantCommandOptions.variantScoreIndexCommandOptions);
        variantSubCommands.addCommand(SCORE_REMOVE_COMMAND, variantCommandOptions.variantScoreRemoveCommandOptions);
        variantSubCommands.addCommand(SAMPLE_INDEX_COMMAND, variantCommandOptions.sampleIndexCommandOptions);
        variantSubCommands.addCommand(FAMILY_INDEX_COMMAND, variantCommandOptions.familyIndexCommandOptions);
        variantSubCommands.addCommand("annotate", variantCommandOptions.annotateVariantCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_SAVE_COMMAND, variantCommandOptions.annotationSaveSnapshotCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_DELETE_COMMAND, variantCommandOptions.annotationDeleteCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_QUERY_COMMAND, variantCommandOptions.annotationQueryCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_METADATA_COMMAND, variantCommandOptions.annotationMetadataCommandOptions);
        variantSubCommands.addCommand(FILL_GAPS_COMMAND, variantCommandOptions.fillGapsVariantCommandOptions);
        variantSubCommands.addCommand(FILL_MISSING_COMMAND, variantCommandOptions.fillMissingCommandOptions);
        variantSubCommands.addCommand("query", variantCommandOptions.queryVariantCommandOptions);
        variantSubCommands.addCommand("export-frequencies", variantCommandOptions.exportVariantStatsCommandOptions);
        variantSubCommands.addCommand("import", variantCommandOptions.importVariantCommandOptions);
        variantSubCommands.addCommand("ibs", variantCommandOptions.ibsVariantCommandOptions);
        variantSubCommands.addCommand("samples", variantCommandOptions.samplesFilterCommandOptions);
        variantSubCommands.addCommand("histogram", variantCommandOptions.histogramCommandOptions);

        alignmentCommandOptions = new AlignmentCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("alignment", alignmentCommandOptions);
        JCommander alignmentSubCommands = jCommander.getCommands().get("alignment");
        alignmentSubCommands.addCommand("index", alignmentCommandOptions.indexAlignmentCommandOptions);
        alignmentSubCommands.addCommand("query", alignmentCommandOptions.queryAlignmentCommandOptions);
        alignmentSubCommands.addCommand("stats", alignmentCommandOptions.statsAlignmentCommandOptions);
        alignmentSubCommands.addCommand("coverage", alignmentCommandOptions.coverageAlignmentCommandOptions);
//        alignmentSubCommands.addCommand("annotate", alignmentCommandOptions.annotateVariantCommandOptions);

        toolsCommandOptions = new ToolsCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("tools", toolsCommandOptions);
        JCommander toolsSubCommands = jCommander.getCommands().get("tools");
        toolsSubCommands.addCommand("list", toolsCommandOptions.listToolCommandOptions);
        toolsSubCommands.addCommand("show", toolsCommandOptions.showToolCommandOptions);
        toolsSubCommands.addCommand("execute", toolsCommandOptions.executeToolCommandOptions);

        interpretationCommandOptions = new InterpretationCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("interpretation", interpretationCommandOptions);
        JCommander interpretationSubCommands = jCommander.getCommands().get("interpretation");
        interpretationSubCommands.addCommand("team", interpretationCommandOptions.teamCommandOptions);
        interpretationSubCommands.addCommand("tiering", interpretationCommandOptions.tieringCommandOptions);

    }

    @Override
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

    /*
     * Expression CLI options
     */
    @Parameters(commandNames = {"expression"}, commandDescription = "Implement gene expression analysis tools")
    public class ExpressionCommandOptions extends CommandOptions {

        DiffExpressionCommandOptions diffExpressionCommandOptions;
        ClusteringExpressionCommandOptions clusteringExpressionCommandOptions;

        GeneralCliOptions.CommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

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

        GeneralCliOptions.CommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        public FunctionalCommandOptions() {
            this.fatigoFunctionalCommandOptions = new FatigoFunctionalCommandOptions();
            this.genesetFunctionalCommandOptions = new GenesetFunctionalCommandOptions();
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
        public GeneralCliOptions.CommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--filter"}, description = "Query filter for data")
        public String filter;
    }

    @Parameters(commandNames = {"clustering"}, commandDescription = "Print summary stats for an user")
    public class ClusteringExpressionCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;
    }



    /*
     * USER SUB-COMMANDS
     */

    @Parameters(commandNames = {"fatigo"}, commandDescription = "Create a new user in Catalog database and the workspace")
    public class FatigoFunctionalCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--user-id"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        public String userId;

    }

    @Parameters(commandNames = {"gene-set"}, commandDescription = "Delete the user Catalog database entry and the workspace")
    public class GenesetFunctionalCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = AnalysisCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--user-id"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        public String userId;

    }

    /*
     *  Variant SUB-COMMANDS
     */

    public class QueryCommandOptions {

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

        @Parameter(names = {"--sort"}, description = "Sort the output elements.")
        public boolean sort;

        @Parameter(names = {"--count"}, description = "Count results. Do not return elements.", required = false, arity = 0)
        public boolean count;

    }

    @Override
    public void printUsage() {
        String parsedCommand = getCommand();
        if (parsedCommand.isEmpty()) {
            System.err.println("");
            System.err.println("Program:     OpenCGA Analysis (OpenCB)");
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

    public GeneralCliOptions.CommonCommandOptions getCommonOptions() {
        return commonCommandOptions;
    }

    public org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions getVariantCommandOptions() {
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

    public InterpretationCommandOptions getInterpretationCommandOptions() {
        return interpretationCommandOptions;
    }
}
