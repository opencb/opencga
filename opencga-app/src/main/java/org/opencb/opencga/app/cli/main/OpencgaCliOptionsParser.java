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

package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.JCommander;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.CliOptionsParser;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions;
import org.opencb.opencga.app.cli.internal.options.VariantCommandOptions;
import org.opencb.opencga.app.cli.main.options.*;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.*;

import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.BwaCommandOptions.BWA_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.DeeptoolsCommandOptions.DEEPTOOLS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.FastqcCommandOptions.FASTQC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.PicardCommandOptions.PICARD_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.SamtoolsCommandOptions.SAMTOOLS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsCommandOptions.COHORT_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyQcCommandOptions.FAMILY_QC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GatkCommandOptions.GATK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GwasCommandOptions.GWAS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.IndividualQcCommandOptions.INDIVIDUAL_QC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.InferredSexCommandOptions.INFERRED_SEX_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.JulieRunCommandOptions.JULIE_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.KnockoutCommandOptions.KNOCKOUT_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.MendelianErrorCommandOptions.MENDELIAN_ERROR_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.MutationalSignatureCommandOptions.MUTATIONAL_SIGNATURE_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.PlinkCommandOptions.PLINK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RelatednessCommandOptions.RELATEDNESS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RvtestsCommandOptions.RVTESTS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleEligibilityCommandOptions.SAMPLE_ELIGIBILITY_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleQcCommandOptions.SAMPLE_QC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsCommandOptions.SAMPLE_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantExportCommandOptions.EXPORT_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantIndexCommandOptions.INDEX_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSampleQueryCommandOptions.SAMPLE_QUERY_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSamplesFilterCommandOptions.SAMPLE_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantStatsCommandOptions.STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationCancerTieringCommandOptions.CANCER_TIERING_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationTeamCommandOptions.TEAM_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationTieringCommandOptions.TIERING_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationZettaCommandOptions.ZETTA_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.VariantActionableCommandOptions.VARIANT_ACTIONABLE_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.VariantQueryCommandOptions.VARIANT_QUERY_COMMAND;
import static org.opencb.opencga.app.cli.main.options.OperationsCommandOptions.*;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions.ANNOTATION_METADATA_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions.ANNOTATION_QUERY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantDeleteCommandOptions.VARIANT_DELETE_COMMAND;

/**
 * Created by imedina on AdminMain.
 */
public class OpencgaCliOptionsParser extends CliOptionsParser {

    private final GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    private final GeneralCliOptions.DataModelOptions dataModelOptions;
    private final GeneralCliOptions.NumericOptions numericOptions;

    // Catalog commands
    private final UserCommandOptions usersCommandOptions;
    private final ProjectCommandOptions projectCommandOptions;
    private final StudyCommandOptions studyCommandOptions;
    private final FileCommandOptions fileCommandOptions;
    private final JobCommandOptions jobCommandOptions;
    private final IndividualCommandOptions individualCommandOptions;
    private final SampleCommandOptions sampleCommandOptions;
    private final CohortCommandOptions cohortCommandOptions;
    private final FamilyCommandOptions familyCommandOptions;
    private final PanelCommandOptions panelCommandOptions;
    private final MetaCommandOptions metaCommandOptions;
    private ToolCommandOptions toolCommandOptions;

    // Analysis commands
    private final AlignmentCommandOptions alignmentCommandOptions;
    private final VariantCommandOptions variantCommandOptions;
    private final ClinicalCommandOptions clinicalCommandOptions;

    private final OperationsCommandOptions operationsCommandOptions;

    enum OutputFormat {IDS, ID_CSV, NAME_ID_MAP, ID_LIST, RAW, PRETTY_JSON, PLAIN_JSON}

    public OpencgaCliOptionsParser() {
        jCommander.setExpandAtSign(false);

        commonCommandOptions = new GeneralCliOptions.CommonCommandOptions();
        dataModelOptions = new GeneralCliOptions.DataModelOptions();
        numericOptions = new GeneralCliOptions.NumericOptions();

        usersCommandOptions = new UserCommandOptions(this.commonCommandOptions, this.dataModelOptions, this.numericOptions, this.jCommander);
        jCommander.addCommand("users", usersCommandOptions);
        JCommander userSubCommands = jCommander.getCommands().get("users");
        userSubCommands.addCommand("create", usersCommandOptions.createCommandOptions);
        userSubCommands.addCommand("info", usersCommandOptions.infoCommandOptions);
        userSubCommands.addCommand("update", usersCommandOptions.updateCommandOptions);
        userSubCommands.addCommand("password", usersCommandOptions.changePasswordCommandOptions);
        userSubCommands.addCommand("projects", usersCommandOptions.projectsCommandOptions);
        userSubCommands.addCommand("login", usersCommandOptions.loginCommandOptions);
        userSubCommands.addCommand("logout", usersCommandOptions.logoutCommandOptions);
        userSubCommands.addCommand("template", usersCommandOptions.templateCommandOptions);

        projectCommandOptions = new ProjectCommandOptions(this.commonCommandOptions, this.dataModelOptions, this.numericOptions, jCommander);
        jCommander.addCommand("projects", projectCommandOptions);
        JCommander projectSubCommands = jCommander.getCommands().get("projects");
        projectSubCommands.addCommand("create", projectCommandOptions.createCommandOptions);
        projectSubCommands.addCommand("info", projectCommandOptions.infoCommandOptions);
        projectSubCommands.addCommand("search", projectCommandOptions.searchCommandOptions);
        projectSubCommands.addCommand("studies", projectCommandOptions.studiesCommandOptions);
        projectSubCommands.addCommand("update", projectCommandOptions.updateCommandOptions);

        studyCommandOptions = new StudyCommandOptions(this.commonCommandOptions, this.dataModelOptions, this.numericOptions, jCommander);
        jCommander.addCommand("studies", studyCommandOptions);
        JCommander studySubCommands = jCommander.getCommands().get("studies");
        studySubCommands.addCommand("create", studyCommandOptions.createCommandOptions);
        studySubCommands.addCommand("info", studyCommandOptions.infoCommandOptions);
        studySubCommands.addCommand("search", studyCommandOptions.searchCommandOptions);
        studySubCommands.addCommand("stats", studyCommandOptions.statsCommandOptions);
        studySubCommands.addCommand("update", studyCommandOptions.updateCommandOptions);
        studySubCommands.addCommand("groups", studyCommandOptions.groupsCommandOptions);
        studySubCommands.addCommand("groups-create", studyCommandOptions.groupsCreateCommandOptions);
        studySubCommands.addCommand("groups-delete", studyCommandOptions.groupsDeleteCommandOptions);
        studySubCommands.addCommand("groups-update", studyCommandOptions.groupsUpdateCommandOptions);
        studySubCommands.addCommand("acl", studyCommandOptions.aclsCommandOptions);
        studySubCommands.addCommand("acl-update", studyCommandOptions.aclsUpdateCommandOptions);
        studySubCommands.addCommand("variable-sets", studyCommandOptions.variableSetsCommandOptions);
        studySubCommands.addCommand("variable-sets-update", studyCommandOptions.variableSetsUpdateCommandOptions);
        studySubCommands.addCommand("variable-sets-variables-update", studyCommandOptions.variablesUpdateCommandOptions);
        studySubCommands.addCommand("variable-sets-variables-update", studyCommandOptions.variablesUpdateCommandOptions);

        fileCommandOptions = new FileCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("files", fileCommandOptions);
        JCommander fileSubCommands = jCommander.getCommands().get("files");
//        fileSubCommands.addCommand("copy", fileCommandOptions.copyCommandOptions);
        fileSubCommands.addCommand("create", fileCommandOptions.createCommandOptions);
        fileSubCommands.addCommand("info", fileCommandOptions.infoCommandOptions);
        fileSubCommands.addCommand("download", fileCommandOptions.downloadCommandOptions);
        fileSubCommands.addCommand("grep", fileCommandOptions.grepCommandOptions);
        fileSubCommands.addCommand("search", fileCommandOptions.searchCommandOptions);
        fileSubCommands.addCommand("list", fileCommandOptions.listCommandOptions);
        fileSubCommands.addCommand("tree", fileCommandOptions.treeCommandOptions);
//        fileSubCommands.addCommand("index", fileCommandOptions.indexCommandOptions);
        fileSubCommands.addCommand("head", fileCommandOptions.headCommandOptions);
        fileSubCommands.addCommand("tail", fileCommandOptions.tailCommandOptions);
//        fileSubCommands.addCommand("fetch", fileCommandOptions.fetchCommandOptions);
        fileSubCommands.addCommand("update", fileCommandOptions.updateCommandOptions);
        fileSubCommands.addCommand("upload", fileCommandOptions.uploadCommandOptions);
        fileSubCommands.addCommand("link", fileCommandOptions.linkCommandOptions);
        fileSubCommands.addCommand("link-run", fileCommandOptions.linkRunCommandOptions);
        fileSubCommands.addCommand("post-link-run", fileCommandOptions.postLinkRunCommandOptions);
        fileSubCommands.addCommand("unlink", fileCommandOptions.unlinkCommandOptions);
//        fileSubCommands.addCommand("relink", fileCommandOptions.relinkCommandOptions);
        fileSubCommands.addCommand("delete", fileCommandOptions.deleteCommandOptions);
//        fileSubCommands.addCommand("refresh", fileCommandOptions.refreshCommandOptions);
        fileSubCommands.addCommand("stats", fileCommandOptions.statsCommandOptions);
        fileSubCommands.addCommand("fetch", fileCommandOptions.fetchCommandOptions);
//        fileSubCommands.addCommand("variants", fileCommandOptions.variantsCommandOptions);
        fileSubCommands.addCommand("acl", fileCommandOptions.aclsCommandOptions);
        fileSubCommands.addCommand("acl-update", fileCommandOptions.aclsUpdateCommandOptions);
        fileSubCommands.addCommand("annotation-sets-update", fileCommandOptions.annotationUpdateCommandOptions);

        jobCommandOptions = new JobCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("jobs", jobCommandOptions);
        JCommander jobSubCommands = jCommander.getCommands().get("jobs");
        jobSubCommands.addCommand("create", jobCommandOptions.createCommandOptions);
        jobSubCommands.addCommand("retry", jobCommandOptions.retryCommandOptions);
        jobSubCommands.addCommand("info", jobCommandOptions.infoCommandOptions);
        jobSubCommands.addCommand("search", jobCommandOptions.searchCommandOptions);
        jobSubCommands.addCommand("top", jobCommandOptions.topCommandOptions);
        jobSubCommands.addCommand("log", jobCommandOptions.logCommandOptions);
        jobSubCommands.addCommand("delete", jobCommandOptions.deleteCommandOptions);
        jobSubCommands.addCommand("acl", jobCommandOptions.aclsCommandOptions);
        jobSubCommands.addCommand("acl-update", jobCommandOptions.aclsUpdateCommandOptions);
        // jobSubCommands.addCommand("status", jobCommandOptions.statusCommandOptions);

        individualCommandOptions = new IndividualCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("individuals", individualCommandOptions);
        JCommander individualSubCommands = jCommander.getCommands().get("individuals");
        individualSubCommands.addCommand("create", individualCommandOptions.createCommandOptions);
        individualSubCommands.addCommand("info", individualCommandOptions.infoCommandOptions);
        individualSubCommands.addCommand("search", individualCommandOptions.searchCommandOptions);
        individualSubCommands.addCommand("update", individualCommandOptions.updateCommandOptions);
        individualSubCommands.addCommand("delete", individualCommandOptions.deleteCommandOptions);
        individualSubCommands.addCommand("samples", individualCommandOptions.sampleCommandOptions);
        individualSubCommands.addCommand("stats", individualCommandOptions.statsCommandOptions);
        individualSubCommands.addCommand("acl", individualCommandOptions.aclsCommandOptions);
        individualSubCommands.addCommand("acl-update", individualCommandOptions.aclsUpdateCommandOptions);
        individualSubCommands.addCommand("annotation-sets-update", individualCommandOptions.annotationUpdateCommandOptions);

        familyCommandOptions = new FamilyCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("families", familyCommandOptions);
        JCommander familySubCommands = jCommander.getCommands().get("families");
        familySubCommands.addCommand("create", familyCommandOptions.createCommandOptions);
        familySubCommands.addCommand("info", familyCommandOptions.infoCommandOptions);
        familySubCommands.addCommand("search", familyCommandOptions.searchCommandOptions);
        familySubCommands.addCommand("stats", familyCommandOptions.statsCommandOptions);
//        familySubCommands.addCommand("update", familyCommandOptions.updateCommandOptions);
        familySubCommands.addCommand("acl", familyCommandOptions.aclsCommandOptions);
        familySubCommands.addCommand("acl-update", familyCommandOptions.aclsUpdateCommandOptions);
        familySubCommands.addCommand("annotation-sets-update", familyCommandOptions.annotationUpdateCommandOptions);

        panelCommandOptions = new PanelCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("panels", panelCommandOptions);
        JCommander panelSubcommands = jCommander.getCommands().get("panels");
        panelSubcommands.addCommand("info", panelCommandOptions.infoCommandOptions);
        panelSubcommands.addCommand("search", panelCommandOptions.searchCommandOptions);
        panelSubcommands.addCommand("acl", panelCommandOptions.aclsCommandOptions);
        panelSubcommands.addCommand("acl-update", panelCommandOptions.aclsUpdateCommandOptions);

        sampleCommandOptions = new SampleCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("samples", sampleCommandOptions);
        JCommander sampleSubCommands = jCommander.getCommands().get("samples");
        sampleSubCommands.addCommand("create", sampleCommandOptions.createCommandOptions);
        sampleSubCommands.addCommand("load", sampleCommandOptions.loadCommandOptions);
        sampleSubCommands.addCommand("info", sampleCommandOptions.infoCommandOptions);
        sampleSubCommands.addCommand("search", sampleCommandOptions.searchCommandOptions);
        sampleSubCommands.addCommand("update", sampleCommandOptions.updateCommandOptions);
        sampleSubCommands.addCommand("delete", sampleCommandOptions.deleteCommandOptions);
        sampleSubCommands.addCommand("stats", sampleCommandOptions.statsCommandOptions);
        sampleSubCommands.addCommand("acl", sampleCommandOptions.aclsCommandOptions);
        sampleSubCommands.addCommand("acl-update", sampleCommandOptions.aclsUpdateCommandOptions);
        sampleSubCommands.addCommand("annotation-sets-update", sampleCommandOptions.annotationUpdateCommandOptions);

        cohortCommandOptions = new CohortCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("cohorts", cohortCommandOptions);
        JCommander cohortSubCommands = jCommander.getCommands().get("cohorts");
        cohortSubCommands.addCommand("create", cohortCommandOptions.createCommandOptions);
        cohortSubCommands.addCommand("info", cohortCommandOptions.infoCommandOptions);
        cohortSubCommands.addCommand("search", cohortCommandOptions.searchCommandOptions);
        cohortSubCommands.addCommand("update", cohortCommandOptions.updateCommandOptions);
        cohortSubCommands.addCommand("delete", cohortCommandOptions.deleteCommandOptions);
        cohortSubCommands.addCommand("stats", cohortCommandOptions.statsCommandOptions);
        cohortSubCommands.addCommand("acl", cohortCommandOptions.aclsCommandOptions);
        cohortSubCommands.addCommand("acl-update", cohortCommandOptions.aclsUpdateCommandOptions);
        cohortSubCommands.addCommand("annotation-sets-update", cohortCommandOptions.annotationUpdateCommandOptions);

        alignmentCommandOptions = new AlignmentCommandOptions(this.commonCommandOptions, jCommander, true);
        jCommander.addCommand("alignments", alignmentCommandOptions);
        JCommander alignmentSubCommands = jCommander.getCommands().get("alignments");
        alignmentSubCommands.addCommand("index-run", alignmentCommandOptions.indexAlignmentCommandOptions);
        alignmentSubCommands.addCommand("query", alignmentCommandOptions.queryAlignmentCommandOptions);
        alignmentSubCommands.addCommand("qc-run", alignmentCommandOptions.qcAlignmentCommandOptions);
        alignmentSubCommands.addCommand("gene-coverage-stats-run", alignmentCommandOptions.geneCoverageStatsAlignmentCommandOptions);
//        alignmentSubCommands.addCommand("stats-run", alignmentCommandOptions.statsAlignmentCommandOptions);
//        alignmentSubCommands.addCommand("flagstats-run", alignmentCommandOptions.flagStatsAlignmentCommandOptions);
//        alignmentSubCommands.addCommand("fastqcmetrics-run", alignmentCommandOptions.fastQcMetricsAlignmentCommandOptions);
//        alignmentSubCommands.addCommand("hsmetrics-run", alignmentCommandOptions.hsMetricsAlignmentCommandOptions);
        alignmentSubCommands.addCommand("coverage-index-run", alignmentCommandOptions.coverageAlignmentCommandOptions);
        alignmentSubCommands.addCommand("coverage-query", alignmentCommandOptions.coverageQueryAlignmentCommandOptions);
        alignmentSubCommands.addCommand("coverage-ratio", alignmentCommandOptions.coverageRatioAlignmentCommandOptions);
        alignmentSubCommands.addCommand("coverage-stats", alignmentCommandOptions.coverageStatsAlignmentCommandOptions);
        alignmentSubCommands.addCommand(BWA_RUN_COMMAND, alignmentCommandOptions.bwaCommandOptions);
        alignmentSubCommands.addCommand(SAMTOOLS_RUN_COMMAND, alignmentCommandOptions.samtoolsCommandOptions);
        alignmentSubCommands.addCommand(DEEPTOOLS_RUN_COMMAND, alignmentCommandOptions.deeptoolsCommandOptions);
        alignmentSubCommands.addCommand(FASTQC_RUN_COMMAND, alignmentCommandOptions.fastqcCommandOptions);
        alignmentSubCommands.addCommand(PICARD_RUN_COMMAND, alignmentCommandOptions.picardCommandOptions);

        variantCommandOptions = new VariantCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander, true);
        jCommander.addCommand("variant", variantCommandOptions);
        JCommander variantSubCommands = jCommander.getCommands().get("variant");
        variantSubCommands.addCommand(INDEX_RUN_COMMAND, variantCommandOptions.indexVariantCommandOptions);
        variantSubCommands.addCommand(VARIANT_DELETE_COMMAND, variantCommandOptions.variantDeleteCommandOptions);
        variantSubCommands.addCommand("query", variantCommandOptions.queryVariantCommandOptions);
        variantSubCommands.addCommand(EXPORT_RUN_COMMAND, variantCommandOptions.exportVariantCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_QUERY_COMMAND, variantCommandOptions.annotationQueryCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_METADATA_COMMAND, variantCommandOptions.annotationMetadataCommandOptions);
        variantSubCommands.addCommand(STATS_RUN_COMMAND, variantCommandOptions.statsVariantCommandOptions);
        variantSubCommands.addCommand(SAMPLE_QUERY_COMMAND, variantCommandOptions.sampleQueryCommandOptions);
        variantSubCommands.addCommand(SAMPLE_RUN_COMMAND, variantCommandOptions.samplesFilterCommandOptions);
        variantSubCommands.addCommand(SAMPLE_VARIANT_STATS_RUN_COMMAND, variantCommandOptions.sampleVariantStatsCommandOptions);
        variantSubCommands.addCommand(COHORT_VARIANT_STATS_RUN_COMMAND, variantCommandOptions.cohortVariantStatsCommandOptions);
        variantSubCommands.addCommand(GWAS_RUN_COMMAND, variantCommandOptions.gwasCommandOptions);
        variantSubCommands.addCommand(KNOCKOUT_RUN_COMMAND, variantCommandOptions.knockoutCommandOptions);
        variantSubCommands.addCommand(SAMPLE_ELIGIBILITY_RUN_COMMAND, variantCommandOptions.sampleEligibilityCommandOptions);
        variantSubCommands.addCommand(MUTATIONAL_SIGNATURE_RUN_COMMAND, variantCommandOptions.mutationalSignatureCommandOptions);
        variantSubCommands.addCommand(MENDELIAN_ERROR_RUN_COMMAND, variantCommandOptions.mendelianErrorCommandOptions);
        variantSubCommands.addCommand(INFERRED_SEX_RUN_COMMAND, variantCommandOptions.inferredSexCommandOptions);
        variantSubCommands.addCommand(RELATEDNESS_RUN_COMMAND, variantCommandOptions.relatednessCommandOptions);
        variantSubCommands.addCommand(FAMILY_QC_RUN_COMMAND, variantCommandOptions.familyQcCommandOptions);
        variantSubCommands.addCommand(INDIVIDUAL_QC_RUN_COMMAND, variantCommandOptions.individualQcCommandOptions);
        variantSubCommands.addCommand(SAMPLE_QC_RUN_COMMAND, variantCommandOptions.sampleQcCommandOptions);
        variantSubCommands.addCommand(PLINK_RUN_COMMAND, variantCommandOptions.plinkCommandOptions);
        variantSubCommands.addCommand(RVTESTS_RUN_COMMAND, variantCommandOptions.rvtestsCommandOptions);
        variantSubCommands.addCommand(GATK_RUN_COMMAND, variantCommandOptions.gatkCommandOptions);

        clinicalCommandOptions = new ClinicalCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("clinical", clinicalCommandOptions);
        JCommander clinicalSubcommands = jCommander.getCommands().get("clinical");
        clinicalSubcommands.addCommand("info", clinicalCommandOptions.infoCommandOptions);
        clinicalSubcommands.addCommand("search", clinicalCommandOptions.searchCommandOptions);
        clinicalSubcommands.addCommand("acl", clinicalCommandOptions.aclsCommandOptions);
        clinicalSubcommands.addCommand("acl-update", clinicalCommandOptions.aclsUpdateCommandOptions);
        clinicalSubcommands.addCommand(VARIANT_QUERY_COMMAND, clinicalCommandOptions.variantQueryCommandOptions);
        clinicalSubcommands.addCommand(VARIANT_ACTIONABLE_COMMAND, clinicalCommandOptions.variantActionableCommandOptions);
        clinicalSubcommands.addCommand(TIERING_RUN_COMMAND, clinicalCommandOptions.tieringCommandOptions);
        clinicalSubcommands.addCommand(TEAM_RUN_COMMAND, clinicalCommandOptions.teamCommandOptions);
        clinicalSubcommands.addCommand(ZETTA_RUN_COMMAND, clinicalCommandOptions.zettaCommandOptions);
        clinicalSubcommands.addCommand(CANCER_TIERING_RUN_COMMAND, clinicalCommandOptions.cancerTieringCommandOptions);

        operationsCommandOptions = new OperationsCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand(OPERATIONS_COMMAND, operationsCommandOptions);
        JCommander operationsSubCommands = jCommander.getCommands().get(OPERATIONS_COMMAND);
        operationsSubCommands.addCommand(VARIANT_CONFIGURE, operationsCommandOptions.variantConfigure);
        operationsSubCommands.addCommand(VARIANT_INDEX_LAUNCHER, operationsCommandOptions.variantIndexLauncher);
        operationsSubCommands.addCommand(VARIANT_SECONDARY_INDEX, operationsCommandOptions.variantSecondaryIndex);
        operationsSubCommands.addCommand(VARIANT_SECONDARY_INDEX_DELETE, operationsCommandOptions.variantSecondaryIndexDelete);
        operationsSubCommands.addCommand(VARIANT_ANNOTATION_INDEX, operationsCommandOptions.variantAnnotation);
        operationsSubCommands.addCommand(VARIANT_ANNOTATION_SAVE, operationsCommandOptions.variantAnnotationSave);
        operationsSubCommands.addCommand(VARIANT_ANNOTATION_DELETE, operationsCommandOptions.variantAnnotationDelete);
        operationsSubCommands.addCommand(VARIANT_SCORE_INDEX, operationsCommandOptions.variantScoreIndex);
        operationsSubCommands.addCommand(VARIANT_SCORE_DELETE, operationsCommandOptions.variantScoreDelete);
        operationsSubCommands.addCommand(VARIANT_FAMILY_INDEX, operationsCommandOptions.variantFamilyIndex);
        operationsSubCommands.addCommand(VARIANT_SAMPLE_INDEX, operationsCommandOptions.variantSampleIndex);
        operationsSubCommands.addCommand(VARIANT_SAMPLE_INDEX_CONFIGURE, operationsCommandOptions.variantSampleIndexConfigure);
        operationsSubCommands.addCommand(VARIANT_AGGREGATE, operationsCommandOptions.variantAggregate);
        operationsSubCommands.addCommand(VARIANT_FAMILY_AGGREGATE, operationsCommandOptions.variantAggregateFamily);
        operationsSubCommands.addCommand(JULIE_RUN_COMMAND, operationsCommandOptions.julieRun);

        metaCommandOptions = new MetaCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("meta", metaCommandOptions);
        JCommander metaSubCommands = jCommander.getCommands().get("meta");
        metaSubCommands.addCommand("about", metaCommandOptions.aboutCommandOptions);
        metaSubCommands.addCommand("status", metaCommandOptions.statusCommandOptions);
        metaSubCommands.addCommand("ping", metaCommandOptions.pingCommandOptions);
        metaSubCommands.addCommand("api", metaCommandOptions.apiCommandOptions);
    }

    @Override
    public boolean isHelp() {
        String parsedCommand = jCommander.getParsedCommand();
        if (parsedCommand != null) {
            JCommander jCommander2 = jCommander.getCommands().get(parsedCommand);
            List<Object> objects = jCommander2.getObjects();
            if (!objects.isEmpty() && objects.get(0) instanceof AdminCliOptionsParser.AdminCommonCommandOptions) {
                return ((AdminCliOptionsParser.AdminCommonCommandOptions) objects.get(0)).commonOptions.help;
            }
        }
        return commonCommandOptions.help;
    }

    @Override
    public void printUsage() {
        String parsedCommand = getCommand();
        if (parsedCommand.isEmpty()) {
            System.err.println("");
            System.err.println("Program:     OpenCGA (OpenCB)");
            System.err.println("Version:     " + GitRepositoryState.get().getBuildVersion());
            System.err.println("Git commit:  " + GitRepositoryState.get().getCommitId());
            System.err.println("Description: Big Data platform for processing and analysing NGS data");
            System.err.println("");
            System.err.println("Usage:       opencga.sh [-h|--help] [--version] <command> [options]");
            System.err.println("");
            printMainUsage();
            System.err.println("");
        } else {
            String parsedSubCommand = getSubCommand();
            if (parsedSubCommand.isEmpty()) {
                System.err.println("");
                System.err.println("Usage:   opencga.sh " + parsedCommand + " <subcommand> [options]");
                System.err.println("");
                System.err.println("Subcommands:");
                printCommands(jCommander.getCommands().get(parsedCommand));
                System.err.println("");
            } else {
                System.err.println("");
                System.err.println("Usage:   opencga.sh " + parsedCommand + " " + parsedSubCommand + " [options]");
                System.err.println("");
                System.err.println("Options:");
                CommandLineUtils.printCommandUsage(jCommander.getCommands().get(parsedCommand).getCommands().get(parsedSubCommand));
                System.err.println("");
            }
        }
    }

    @Override
    protected void printMainUsage() {
        Set<String> analysisCommands = new HashSet<>(Arrays.asList("alignments", "variant", "clinical"));
        Set<String> operationsCommands = new HashSet<>(Collections.singletonList(OPERATIONS_COMMAND));

        System.err.println("Catalog commands:");
        for (String command : jCommander.getCommands().keySet()) {
            if (!analysisCommands.contains(command) && !operationsCommands.contains(command)) {
                System.err.printf("%14s  %s\n", command, jCommander.getCommandDescription(command));
            }
        }

        System.err.println("");
        System.err.println("Analysis commands:");
        for (String command : jCommander.getCommands().keySet()) {
            if (analysisCommands.contains(command)) {
                System.err.printf("%14s  %s\n", command, jCommander.getCommandDescription(command));
            }
        }

        System.err.println("");
        System.err.println("Operation commands:");
        for (String command : jCommander.getCommands().keySet()) {
            if (operationsCommands.contains(command)) {
                System.err.printf("%14s  %s\n", command, jCommander.getCommandDescription(command));
            }
        }
    }

    public GeneralCliOptions.GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    public GeneralCliOptions.CommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }

    public UserCommandOptions getUsersCommandOptions() {
        return usersCommandOptions;
    }

    public ProjectCommandOptions getProjectCommandOptions() {
        return projectCommandOptions;
    }

    public StudyCommandOptions getStudyCommandOptions() {
        return studyCommandOptions;
    }

    public FileCommandOptions getFileCommands() {
        return fileCommandOptions;
    }

    public JobCommandOptions getJobsCommands() {
        return jobCommandOptions;
    }

    public IndividualCommandOptions getIndividualsCommands() {
        return individualCommandOptions;
    }

    public SampleCommandOptions getSampleCommands() {
        return sampleCommandOptions;
    }

    public CohortCommandOptions getCohortCommands() {
        return cohortCommandOptions;
    }

    public FamilyCommandOptions getFamilyCommands() {
        return familyCommandOptions;
    }

    public PanelCommandOptions getPanelCommands() {
        return panelCommandOptions;
    }

    public ToolCommandOptions getToolCommands() {
        return toolCommandOptions;
    }

    public AlignmentCommandOptions getAlignmentCommands() {
        return alignmentCommandOptions;
    }

    public VariantCommandOptions getVariantCommands() {
        return variantCommandOptions;
    }

    public ClinicalCommandOptions getClinicalCommandOptions() {
        return clinicalCommandOptions;
    }

    public OperationsCommandOptions getOperationsCommands() {
        return operationsCommandOptions;
    }

    public MetaCommandOptions getMetaCommandOptions() {
        return metaCommandOptions;
    }
}
