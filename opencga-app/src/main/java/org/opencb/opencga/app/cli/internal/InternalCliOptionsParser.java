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

package org.opencb.opencga.app.cli.internal;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.CliOptionsParser;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.internal.options.*;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.List;

import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.BwaCommandOptions.BWA_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.DeeptoolsCommandOptions.DEEPTOOLS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.FastqcCommandOptions.FASTQC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.SamtoolsCommandOptions.SAMTOOLS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsCommandOptions.COHORT_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsQueryCommandOptions.COHORT_VARIANT_STATS_QUERY_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GatkCommandOptions.GATK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GeneticChecksCommandOptions.GENETIC_CHECKS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GwasCommandOptions.GWAS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.InferredSexCommandOptions.INFERRED_SEX_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.KnockoutCommandOptions.KNOCKOUT_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.MendelianErrorCommandOptions.MENDELIAN_ERROR_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.MutationalSignatureCommandOptions.MUTATIONAL_SIGNATURE_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.PlinkCommandOptions.PLINK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RelatednessCommandOptions.RELATEDNESS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RvtestsCommandOptions.RVTEST_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleEligibilityCommandOptions.SAMPLE_ELIGIBILITY_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsCommandOptions.SAMPLE_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsQueryCommandOptions.SAMPLE_VARIANT_STATS_QUERY_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantAnnotateCommandOptions.ANNOTATION_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantExportCommandOptions.EXPORT_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantExportStatsCommandOptions.STATS_EXPORT_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantIndexCommandOptions.INDEX_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSamplesFilterCommandOptions.SAMPLE_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantScoreDeleteCommandOptions.SCORE_DELETE_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantScoreIndexCommandOptions.SCORE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSecondaryIndexCommandOptions.SECONDARY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSecondaryIndexDeleteCommandOptions.SECONDARY_INDEX_DELETE_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantStatsCommandOptions.STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationCancerTieringCommandOptions.CANCER_TIERING_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationTeamCommandOptions.TEAM_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationTieringCommandOptions.TIERING_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationZettaCommandOptions.ZETTA_RUN_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.AggregateCommandOptions.AGGREGATE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.AggregateFamilyCommandOptions.AGGREGATE_FAMILY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions.ANNOTATION_DELETE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions.ANNOTATION_METADATA_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions.ANNOTATION_QUERY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions.ANNOTATION_SAVE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantDeleteCommandOptions.VARIANT_DELETE_COMMAND;

/**
 * Created by imedina on 02/03/15.
 */
public class InternalCliOptionsParser extends CliOptionsParser {

    private final GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    private final GeneralCliOptions.DataModelOptions dataModelOptions;
    private final GeneralCliOptions.NumericOptions numericOptions;

    private final VariantCommandOptions.VariantQueryCommandOptions variantQueryCommandOptions;

    private ExpressionCommandOptions expressionCommandOptions;
    private FunctionalCommandOptions functionalCommandOptions;
    private org.opencb.opencga.app.cli.internal.options.VariantCommandOptions variantCommandOptions;
//    private VariantCommandOptions variantCommandOptions;
    private ToolsCommandOptions toolsCommandOptions;
    private AlignmentCommandOptions alignmentCommandOptions;
//    private InterpretationCommandOptions interpretationCommandOptions;
    private ClinicalCommandOptions clinicalCommandOptions;
    private FileCommandOptions fileCommandOptions;
    private SampleCommandOptions sampleCommandOptions;
    private FamilyCommandOptions familyCommandOptions;
    private CohortCommandOptions cohortCommandOptions;
    private IndividualCommandOptions individualCommandOptions;
    private JobCommandOptions jobCommandOptions;


    public InternalCliOptionsParser() {
        jCommander.setProgramName("opencga-analysis.sh");

        commonCommandOptions = new GeneralCliOptions.CommonCommandOptions();
        dataModelOptions = new GeneralCliOptions.DataModelOptions();
        numericOptions = new GeneralCliOptions.NumericOptions();

        variantQueryCommandOptions = new VariantCommandOptions(commonCommandOptions, dataModelOptions, numericOptions, jCommander, false)
                .new VariantQueryCommandOptions();

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

        variantCommandOptions = new org.opencb.opencga.app.cli.internal.options.VariantCommandOptions(commonCommandOptions,
                dataModelOptions, numericOptions, jCommander, false);
        jCommander.addCommand("variant", variantCommandOptions);
        JCommander variantSubCommands = jCommander.getCommands().get("variant");
        variantSubCommands.addCommand(INDEX_RUN_COMMAND, variantCommandOptions.indexVariantCommandOptions);
        variantSubCommands.addCommand(VARIANT_DELETE_COMMAND, variantCommandOptions.variantDeleteCommandOptions);
        variantSubCommands.addCommand(SECONDARY_INDEX_COMMAND, variantCommandOptions.variantSecondaryIndexCommandOptions);
        variantSubCommands.addCommand(SECONDARY_INDEX_DELETE_COMMAND, variantCommandOptions.variantSecondaryIndexDeleteCommandOptions);
        variantSubCommands.addCommand(STATS_RUN_COMMAND, variantCommandOptions.statsVariantCommandOptions);
        variantSubCommands.addCommand(SCORE_INDEX_COMMAND, variantCommandOptions.variantScoreIndexCommandOptions);
        variantSubCommands.addCommand(SCORE_DELETE_COMMAND, variantCommandOptions.variantScoreDeleteCommandOptions);
        variantSubCommands.addCommand(SAMPLE_INDEX_COMMAND, variantCommandOptions.sampleIndexCommandOptions);
        variantSubCommands.addCommand(FAMILY_INDEX_COMMAND, variantCommandOptions.familyIndexCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_INDEX_COMMAND, variantCommandOptions.annotateVariantCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_SAVE_COMMAND, variantCommandOptions.annotationSaveSnapshotCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_DELETE_COMMAND, variantCommandOptions.annotationDeleteCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_QUERY_COMMAND, variantCommandOptions.annotationQueryCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_METADATA_COMMAND, variantCommandOptions.annotationMetadataCommandOptions);
        variantSubCommands.addCommand(AGGREGATE_FAMILY_COMMAND, variantCommandOptions.fillGapsVariantCommandOptions);
        variantSubCommands.addCommand(AGGREGATE_COMMAND, variantCommandOptions.aggregateCommandOptions);
        variantSubCommands.addCommand("query", variantCommandOptions.queryVariantCommandOptions);
        variantSubCommands.addCommand(EXPORT_RUN_COMMAND, variantCommandOptions.exportVariantCommandOptions);
        variantSubCommands.addCommand(STATS_EXPORT_RUN_COMMAND, variantCommandOptions.exportVariantStatsCommandOptions);
        variantSubCommands.addCommand("import", variantCommandOptions.importVariantCommandOptions);
//        variantSubCommands.addCommand("ibs", variantCommandOptions.ibsVariantCommandOptions);
        variantSubCommands.addCommand(SAMPLE_RUN_COMMAND, variantCommandOptions.samplesFilterCommandOptions);
        variantSubCommands.addCommand("histogram", variantCommandOptions.histogramCommandOptions);
        variantSubCommands.addCommand(GWAS_RUN_COMMAND, variantCommandOptions.gwasCommandOptions);
        variantSubCommands.addCommand(SAMPLE_VARIANT_STATS_RUN_COMMAND, variantCommandOptions.sampleVariantStatsCommandOptions);
        variantSubCommands.addCommand(SAMPLE_VARIANT_STATS_QUERY_COMMAND, variantCommandOptions.sampleVariantStatsQueryCommandOptions);
        variantSubCommands.addCommand(COHORT_VARIANT_STATS_RUN_COMMAND, variantCommandOptions.cohortVariantStatsCommandOptions);
        variantSubCommands.addCommand(COHORT_VARIANT_STATS_QUERY_COMMAND, variantCommandOptions.cohortVariantStatsQueryCommandOptions);
        variantSubCommands.addCommand(KNOCKOUT_RUN_COMMAND, variantCommandOptions.knockoutCommandOptions);
        variantSubCommands.addCommand(SAMPLE_ELIGIBILITY_RUN_COMMAND, variantCommandOptions.sampleEligibilityCommandOptions);
        variantSubCommands.addCommand(MUTATIONAL_SIGNATURE_RUN_COMMAND, variantCommandOptions.mutationalSignatureCommandOptions);
        variantSubCommands.addCommand(MENDELIAN_ERROR_RUN_COMMAND, variantCommandOptions.mendelianErrorCommandOptions);
        variantSubCommands.addCommand(INFERRED_SEX_RUN_COMMAND, variantCommandOptions.inferredSexCommandOptions);
        variantSubCommands.addCommand(RELATEDNESS_RUN_COMMAND, variantCommandOptions.relatednessCommandOptions);
        variantSubCommands.addCommand(GENETIC_CHECKS_RUN_COMMAND, variantCommandOptions.geneticChecksCommandOptions);
        variantSubCommands.addCommand(PLINK_RUN_COMMAND, variantCommandOptions.plinkCommandOptions);
        variantSubCommands.addCommand(RVTEST_RUN_COMMAND, variantCommandOptions.rvtestsCommandOptions);
        variantSubCommands.addCommand(GATK_RUN_COMMAND, variantCommandOptions.gatkCommandOptions);

        alignmentCommandOptions = new AlignmentCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("alignment", alignmentCommandOptions);
        JCommander alignmentSubCommands = jCommander.getCommands().get("alignment");
        alignmentSubCommands.addCommand("index", alignmentCommandOptions.indexAlignmentCommandOptions);
        alignmentSubCommands.addCommand("query", alignmentCommandOptions.queryAlignmentCommandOptions);
        alignmentSubCommands.addCommand("stats-run", alignmentCommandOptions.statsAlignmentCommandOptions);
        alignmentSubCommands.addCommand("stats-info", alignmentCommandOptions.statsInfoAlignmentCommandOptions);
        alignmentSubCommands.addCommand("coverage-run", alignmentCommandOptions.coverageAlignmentCommandOptions);
//        alignmentSubCommands.addCommand("annotate", alignmentCommandOptions.annotateVariantCommandOptions);
        alignmentSubCommands.addCommand(BWA_RUN_COMMAND, alignmentCommandOptions.bwaCommandOptions);
        alignmentSubCommands.addCommand(SAMTOOLS_RUN_COMMAND, alignmentCommandOptions.samtoolsCommandOptions);
        alignmentSubCommands.addCommand(DEEPTOOLS_RUN_COMMAND, alignmentCommandOptions.deeptoolsCommandOptions);
        alignmentSubCommands.addCommand(FASTQC_RUN_COMMAND, alignmentCommandOptions.fastqcCommandOptions);

        toolsCommandOptions = new ToolsCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("tools", toolsCommandOptions);
        JCommander toolsSubCommands = jCommander.getCommands().get("tools");
        toolsSubCommands.addCommand("list", toolsCommandOptions.listToolCommandOptions);
//        toolsSubCommands.addCommand("show", toolsCommandOptions.showToolCommandOptions);
        toolsSubCommands.addCommand("execute-tool", toolsCommandOptions.executeToolCommandOptions);
        toolsSubCommands.addCommand("execute-job", toolsCommandOptions.executeJobCommandOptions);

//        interpretationCommandOptions = new InterpretationCommandOptions(commonCommandOptions, variantQueryCommandOptions, jCommander);
//        jCommander.addCommand("interpretation", interpretationCommandOptions);
//        JCommander interpretationSubCommands = jCommander.getCommands().get("interpretation");
//        interpretationSubCommands.addCommand("team", interpretationCommandOptions.teamCommandOptions);
////        interpretationSubCommands.addCommand("tiering", interpretationCommandOptions.tieringCommandOptions);

        clinicalCommandOptions = new ClinicalCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("clinical", clinicalCommandOptions);
        JCommander clinicalSubCommands = jCommander.getCommands().get("clinical");
        clinicalSubCommands.addCommand(TIERING_RUN_COMMAND, clinicalCommandOptions.tieringCommandOptions);
        clinicalSubCommands.addCommand(TEAM_RUN_COMMAND, clinicalCommandOptions.teamCommandOptions);
        clinicalSubCommands.addCommand(ZETTA_RUN_COMMAND, clinicalCommandOptions.zettaCommandOptions);
        clinicalSubCommands.addCommand(CANCER_TIERING_RUN_COMMAND, clinicalCommandOptions.cancerTieringCommandOptions);

        fileCommandOptions = new FileCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("files", fileCommandOptions);
        JCommander fileSubCommands = jCommander.getCommands().get("files");
        fileSubCommands.addCommand("delete", fileCommandOptions.deleteCommandOptions);
        fileSubCommands.addCommand("unlink", fileCommandOptions.unlinkCommandOptions);
        fileSubCommands.addCommand("fetch", fileCommandOptions.fetchCommandOptions);
        fileSubCommands.addCommand("secondary-index", fileCommandOptions.secondaryIndex);
        fileSubCommands.addCommand("tsv-load", fileCommandOptions.tsvLoad);

        sampleCommandOptions = new SampleCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("samples", sampleCommandOptions);
        JCommander sampleSubCommands = jCommander.getCommands().get("samples");
        sampleSubCommands.addCommand("secondary-index", sampleCommandOptions.secondaryIndex);
        sampleSubCommands.addCommand("tsv-load", sampleCommandOptions.tsvLoad);

        individualCommandOptions = new IndividualCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("individuals", individualCommandOptions);
        JCommander individualSubCommands = jCommander.getCommands().get("individuals");
        individualSubCommands.addCommand("secondary-index", individualCommandOptions.secondaryIndex);
        individualSubCommands.addCommand("tsv-load", individualCommandOptions.tsvLoad);

        cohortCommandOptions = new CohortCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("cohorts", cohortCommandOptions);
        JCommander cohortSubCommands = jCommander.getCommands().get("cohorts");
        cohortSubCommands.addCommand("secondary-index", cohortCommandOptions.secondaryIndex);
        cohortSubCommands.addCommand("tsv-load", cohortCommandOptions.tsvLoad);

        familyCommandOptions = new FamilyCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("families", familyCommandOptions);
        JCommander familySubCommands = jCommander.getCommands().get("families");
        familySubCommands.addCommand("secondary-index", familyCommandOptions.secondaryIndex);
        familySubCommands.addCommand("tsv-load", familyCommandOptions.tsvLoad);

        jobCommandOptions = new JobCommandOptions(commonCommandOptions, jCommander);
        jCommander.addCommand("jobs", jobCommandOptions);
        JCommander jobSubCommands = jCommander.getCommands().get("jobs");
        jobSubCommands.addCommand("secondary-index", jobCommandOptions.secondaryIndex);
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
            return jCommander.getCommands().get(getCommand()).getCommands().get(getParsedSubCommand());
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

        GeneralCliOptions.CommonCommandOptions commonOptions = InternalCliOptionsParser.this.commonCommandOptions;

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

        GeneralCliOptions.CommonCommandOptions commonOptions = InternalCliOptionsParser.this.commonCommandOptions;

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
        public GeneralCliOptions.CommonCommandOptions commonOptions = InternalCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--filter"}, description = "Query filter for data")
        public String filter;
    }

    @Parameters(commandNames = {"clustering"}, commandDescription = "Print summary stats for an user")
    public class ClusteringExpressionCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = InternalCliOptionsParser.this.commonCommandOptions;
    }



    /*
     * USER SUB-COMMANDS
     */

    @Parameters(commandNames = {"fatigo"}, commandDescription = "Create a new user in Catalog database and the workspace")
    public class FatigoFunctionalCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = InternalCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--user-id"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        public String userId;

    }

    @Parameters(commandNames = {"gene-set"}, commandDescription = "Delete the user Catalog database entry and the workspace")
    public class GenesetFunctionalCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = InternalCliOptionsParser.this.commonCommandOptions;


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

    public org.opencb.opencga.app.cli.internal.options.VariantCommandOptions getVariantCommandOptions() {
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

    public ClinicalCommandOptions getClinicalCommandOptions() {
        return clinicalCommandOptions;
    }

    public FileCommandOptions getFileCommandOptions() {
        return fileCommandOptions;
    }

    public SampleCommandOptions getSampleCommandOptions() {
        return sampleCommandOptions;
    }

    public FamilyCommandOptions getFamilyCommandOptions() {
        return familyCommandOptions;
    }

    public CohortCommandOptions getCohortCommandOptions() {
        return cohortCommandOptions;
    }

    public IndividualCommandOptions getIndividualCommandOptions() {
        return individualCommandOptions;
    }

    public JobCommandOptions getJobCommandOptions() {
        return jobCommandOptions;
    }
}
