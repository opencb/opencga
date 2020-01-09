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
import org.opencb.opencga.analysis.wrappers.*;
import org.opencb.opencga.app.cli.CliOptionsParser;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions;
import org.opencb.opencga.app.cli.internal.options.VariantCommandOptions;
import org.opencb.opencga.app.cli.main.options.*;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.*;

import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsCommandOptions.COHORT_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GwasCommandOptions.GWAS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.PlinkCommandOptions.PLINK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RvtestsCommandOptions.RVTEST_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsCommandOptions.SAMPLE_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSampleQueryCommandOptions.SAMPLE_QUERY_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSamplesFilterCommandOptions.SAMPLE_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantStatsCommandOptions.STATS_RUN_COMMAND;
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
    private final ClinicalCommandOptions clinicalCommandOptions;
    private final VariableCommandOptions variableCommandOptions;
    private final CohortCommandOptions cohortCommandOptions;
    private final FamilyCommandOptions familyCommandOptions;
    private final PanelCommandOptions panelCommandOptions;
    private ToolCommandOptions toolCommandOptions;

    // Analysis commands
    private final AlignmentCommandOptions alignmentCommandOptions;
    private final VariantCommandOptions variantCommandOptions;

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
        userSubCommands.addCommand("delete", usersCommandOptions.deleteCommandOptions);
        userSubCommands.addCommand("projects", usersCommandOptions.projectsCommandOptions);
        userSubCommands.addCommand("login", usersCommandOptions.loginCommandOptions);
        userSubCommands.addCommand("logout", usersCommandOptions.logoutCommandOptions);

        projectCommandOptions = new ProjectCommandOptions(this.commonCommandOptions, this.dataModelOptions, this.numericOptions, jCommander);
        jCommander.addCommand("projects", projectCommandOptions);
        JCommander projectSubCommands = jCommander.getCommands().get("projects");
        projectSubCommands.addCommand("create", projectCommandOptions.createCommandOptions);
        projectSubCommands.addCommand("info", projectCommandOptions.infoCommandOptions);
        projectSubCommands.addCommand("search", projectCommandOptions.searchCommandOptions);
        projectSubCommands.addCommand("studies", projectCommandOptions.studiesCommandOptions);
        projectSubCommands.addCommand("update", projectCommandOptions.updateCommandOptions);
        projectSubCommands.addCommand("delete", projectCommandOptions.deleteCommandOptions);

        studyCommandOptions = new StudyCommandOptions(this.commonCommandOptions, this.dataModelOptions, this.numericOptions, jCommander);
        jCommander.addCommand("studies", studyCommandOptions);
        JCommander studySubCommands = jCommander.getCommands().get("studies");
        studySubCommands.addCommand("create", studyCommandOptions.createCommandOptions);
        studySubCommands.addCommand("info", studyCommandOptions.infoCommandOptions);
        studySubCommands.addCommand("search", studyCommandOptions.searchCommandOptions);
        studySubCommands.addCommand("stats", studyCommandOptions.statsCommandOptions);
        studySubCommands.addCommand("delete", studyCommandOptions.deleteCommandOptions);
        studySubCommands.addCommand("update", studyCommandOptions.updateCommandOptions);
        studySubCommands.addCommand("scan-files", studyCommandOptions.scanFilesCommandOptions);
        studySubCommands.addCommand("resync-files", studyCommandOptions.resyncFilesCommandOptions);
        studySubCommands.addCommand("groups", studyCommandOptions.groupsCommandOptions);
        studySubCommands.addCommand("groups-create", studyCommandOptions.groupsCreateCommandOptions);
        studySubCommands.addCommand("groups-delete", studyCommandOptions.groupsDeleteCommandOptions);
        studySubCommands.addCommand("groups-update", studyCommandOptions.groupsUpdateCommandOptions);
        studySubCommands.addCommand("members-update", studyCommandOptions.memberGroupUpdateCommandOptions);
        studySubCommands.addCommand("admins-update", studyCommandOptions.adminsGroupUpdateCommandOptions);
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
        fileSubCommands.addCommand("create-folder", fileCommandOptions.createFolderCommandOptions);
        fileSubCommands.addCommand("info", fileCommandOptions.infoCommandOptions);
        fileSubCommands.addCommand("download", fileCommandOptions.downloadCommandOptions);
        fileSubCommands.addCommand("grep", fileCommandOptions.grepCommandOptions);
        fileSubCommands.addCommand("search", fileCommandOptions.searchCommandOptions);
        fileSubCommands.addCommand("list", fileCommandOptions.listCommandOptions);
        fileSubCommands.addCommand("tree", fileCommandOptions.treeCommandOptions);
//        fileSubCommands.addCommand("index", fileCommandOptions.indexCommandOptions);
        fileSubCommands.addCommand("content", fileCommandOptions.contentCommandOptions);
//        fileSubCommands.addCommand("fetch", fileCommandOptions.fetchCommandOptions);
        fileSubCommands.addCommand("update", fileCommandOptions.updateCommandOptions);
        fileSubCommands.addCommand("upload", fileCommandOptions.uploadCommandOptions);
        fileSubCommands.addCommand("link", fileCommandOptions.linkCommandOptions);
        fileSubCommands.addCommand("unlink", fileCommandOptions.unlinkCommandOptions);
//        fileSubCommands.addCommand("relink", fileCommandOptions.relinkCommandOptions);
        fileSubCommands.addCommand("delete", fileCommandOptions.deleteCommandOptions);
//        fileSubCommands.addCommand("refresh", fileCommandOptions.refreshCommandOptions);
        fileSubCommands.addCommand("stats", fileCommandOptions.statsCommandOptions);
//        fileSubCommands.addCommand("variants", fileCommandOptions.variantsCommandOptions);
        fileSubCommands.addCommand("acl", fileCommandOptions.aclsCommandOptions);
        fileSubCommands.addCommand("acl-update", fileCommandOptions.aclsUpdateCommandOptions);
        fileSubCommands.addCommand("annotation-sets-update", fileCommandOptions.annotationUpdateCommandOptions);

        jobCommandOptions = new JobCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("jobs", jobCommandOptions);
        JCommander jobSubCommands = jCommander.getCommands().get("jobs");
        jobSubCommands.addCommand("create", jobCommandOptions.createCommandOptions);
        jobSubCommands.addCommand("info", jobCommandOptions.infoCommandOptions);
        jobSubCommands.addCommand("search", jobCommandOptions.searchCommandOptions);
        jobSubCommands.addCommand("visit", jobCommandOptions.visitCommandOptions);
        jobSubCommands.addCommand("delete", jobCommandOptions.deleteCommandOptions);
        jobSubCommands.addCommand("group-by", jobCommandOptions.groupByCommandOptions);
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
        individualSubCommands.addCommand("group-by", individualCommandOptions.groupByCommandOptions);
        individualSubCommands.addCommand("samples", individualCommandOptions.sampleCommandOptions);
        individualSubCommands.addCommand("stats", individualCommandOptions.statsCommandOptions);
        individualSubCommands.addCommand("acl", individualCommandOptions.aclsCommandOptions);
        individualSubCommands.addCommand("acl-update", individualCommandOptions.aclsUpdateCommandOptions);
        individualSubCommands.addCommand("annotation-sets-create", individualCommandOptions.annotationCreateCommandOptions);
        individualSubCommands.addCommand("annotation-sets", individualCommandOptions.annotationInfoCommandOptions);
        individualSubCommands.addCommand("annotation-sets-search", individualCommandOptions.annotationSearchCommandOptions);
        individualSubCommands.addCommand("annotation-sets-update", individualCommandOptions.annotationUpdateCommandOptions);
        individualSubCommands.addCommand("annotation-sets-delete", individualCommandOptions.annotationDeleteCommandOptions);

        familyCommandOptions = new FamilyCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("families", familyCommandOptions);
        JCommander familySubCommands = jCommander.getCommands().get("families");
        familySubCommands.addCommand("create", familyCommandOptions.createCommandOptions);
        familySubCommands.addCommand("info", familyCommandOptions.infoCommandOptions);
        familySubCommands.addCommand("search", familyCommandOptions.searchCommandOptions);
        familySubCommands.addCommand("group-by", familyCommandOptions.groupByCommandOptions);
        familySubCommands.addCommand("stats", familyCommandOptions.statsCommandOptions);
//        familySubCommands.addCommand("update", familyCommandOptions.updateCommandOptions);
        familySubCommands.addCommand("acl", familyCommandOptions.aclsCommandOptions);
        familySubCommands.addCommand("acl-update", familyCommandOptions.aclsUpdateCommandOptions);
        familySubCommands.addCommand("annotation-sets-create", familyCommandOptions.annotationCreateCommandOptions);
        familySubCommands.addCommand("annotation-sets", familyCommandOptions.annotationInfoCommandOptions);
        familySubCommands.addCommand("annotation-sets-search", familyCommandOptions.annotationSearchCommandOptions);
        familySubCommands.addCommand("annotation-sets-update", familyCommandOptions.annotationUpdateCommandOptions);
        familySubCommands.addCommand("annotation-sets-delete", familyCommandOptions.annotationDeleteCommandOptions);

        clinicalCommandOptions = new ClinicalCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("clinical", clinicalCommandOptions);
        JCommander clinicalSubcommands = jCommander.getCommands().get("clinical");
        clinicalSubcommands.addCommand("info", clinicalCommandOptions.infoCommandOptions);
        clinicalSubcommands.addCommand("search", clinicalCommandOptions.searchCommandOptions);
        clinicalSubcommands.addCommand("group-by", clinicalCommandOptions.groupByCommandOptions);
        clinicalSubcommands.addCommand("acl", clinicalCommandOptions.aclsCommandOptions);
        clinicalSubcommands.addCommand("acl-update", clinicalCommandOptions.aclsUpdateCommandOptions);

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
        sampleSubCommands.addCommand("group-by", sampleCommandOptions.groupByCommandOptions);
        sampleSubCommands.addCommand("individuals", sampleCommandOptions.individualCommandOptions);
        sampleSubCommands.addCommand("stats", sampleCommandOptions.statsCommandOptions);
        sampleSubCommands.addCommand("acl", sampleCommandOptions.aclsCommandOptions);
        sampleSubCommands.addCommand("acl-update", sampleCommandOptions.aclsUpdateCommandOptions);
        sampleSubCommands.addCommand("annotation-sets-create", sampleCommandOptions.annotationCreateCommandOptions);
        sampleSubCommands.addCommand("annotation-sets", sampleCommandOptions.annotationInfoCommandOptions);
        sampleSubCommands.addCommand("annotation-sets-search", sampleCommandOptions.annotationSearchCommandOptions);
        sampleSubCommands.addCommand("annotation-sets-update", sampleCommandOptions.annotationUpdateCommandOptions);
        sampleSubCommands.addCommand("annotation-sets-delete", sampleCommandOptions.annotationDeleteCommandOptions);

        variableCommandOptions = new VariableCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("variables", variableCommandOptions);
        JCommander variableSubCommands = jCommander.getCommands().get("variables");
        variableSubCommands.addCommand("create", variableCommandOptions.createCommandOptions);
        variableSubCommands.addCommand("info", variableCommandOptions.infoCommandOptions);
        variableSubCommands.addCommand("search", variableCommandOptions.searchCommandOptions);
        variableSubCommands.addCommand("delete", variableCommandOptions.deleteCommandOptions);
        variableSubCommands.addCommand("field-add", variableCommandOptions.fieldAddCommandOptions);
        variableSubCommands.addCommand("field-delete", variableCommandOptions.fieldDeleteCommandOptions);
        variableSubCommands.addCommand("field-rename", variableCommandOptions.fieldRenameCommandOptions);

        cohortCommandOptions = new CohortCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("cohorts", cohortCommandOptions);
        JCommander cohortSubCommands = jCommander.getCommands().get("cohorts");
        cohortSubCommands.addCommand("create", cohortCommandOptions.createCommandOptions);
        cohortSubCommands.addCommand("info", cohortCommandOptions.infoCommandOptions);
        cohortSubCommands.addCommand("search", cohortCommandOptions.searchCommandOptions);
        cohortSubCommands.addCommand("samples", cohortCommandOptions.samplesCommandOptions);
        cohortSubCommands.addCommand("update", cohortCommandOptions.updateCommandOptions);
        cohortSubCommands.addCommand("delete", cohortCommandOptions.deleteCommandOptions);
        cohortSubCommands.addCommand("group-by", cohortCommandOptions.groupByCommandOptions);
        cohortSubCommands.addCommand("stats", cohortCommandOptions.statsCommandOptions);
        cohortSubCommands.addCommand("acl", cohortCommandOptions.aclsCommandOptions);
        cohortSubCommands.addCommand("acl-update", cohortCommandOptions.aclsUpdateCommandOptions);
        cohortSubCommands.addCommand("annotation-sets-create", cohortCommandOptions.annotationCreateCommandOptions);
        cohortSubCommands.addCommand("annotation-sets", cohortCommandOptions.annotationInfoCommandOptions);
        cohortSubCommands.addCommand("annotation-sets-search", cohortCommandOptions.annotationSearchCommandOptions);
        cohortSubCommands.addCommand("annotation-sets-update", cohortCommandOptions.annotationUpdateCommandOptions);
        cohortSubCommands.addCommand("annotation-sets-delete", cohortCommandOptions.annotationDeleteCommandOptions);

        alignmentCommandOptions = new AlignmentCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("alignments", alignmentCommandOptions);
        JCommander alignmentSubCommands = jCommander.getCommands().get("alignments");
        alignmentSubCommands.addCommand("index", alignmentCommandOptions.indexAlignmentCommandOptions);
        alignmentSubCommands.addCommand("query", alignmentCommandOptions.queryAlignmentCommandOptions);
        alignmentSubCommands.addCommand("stats-run", alignmentCommandOptions.statsAlignmentCommandOptions);
        alignmentSubCommands.addCommand("stats-info", alignmentCommandOptions.statsInfoAlignmentCommandOptions);
        alignmentSubCommands.addCommand("stats-query", alignmentCommandOptions.statsQueryAlignmentCommandOptions);
        alignmentSubCommands.addCommand("coverage-run", alignmentCommandOptions.coverageAlignmentCommandOptions);
        alignmentSubCommands.addCommand("coverage-query", alignmentCommandOptions.coverageQueryAlignmentCommandOptions);
        alignmentSubCommands.addCommand("coverage-ratio", alignmentCommandOptions.coverageRatioAlignmentCommandOptions);
        alignmentSubCommands.addCommand(BwaWrapperAnalysis.ID, alignmentCommandOptions.bwaCommandOptions);
        alignmentSubCommands.addCommand(SamtoolsWrapperAnalysis.ID, alignmentCommandOptions.samtoolsCommandOptions);
        alignmentSubCommands.addCommand(DeeptoolsWrapperAnalysis.ID, alignmentCommandOptions.deeptoolsCommandOptions);
        alignmentSubCommands.addCommand(FastqcWrapperAnalysis.ID, alignmentCommandOptions.fastqcCommandOptions);

        variantCommandOptions = new VariantCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand("variant", variantCommandOptions);
        JCommander variantSubCommands = jCommander.getCommands().get("variant");
        variantSubCommands.addCommand("index", variantCommandOptions.indexVariantCommandOptions);
        variantSubCommands.addCommand(VARIANT_DELETE_COMMAND, variantCommandOptions.variantDeleteCommandOptions);
        variantSubCommands.addCommand("query", variantCommandOptions.queryVariantCommandOptions);
        variantSubCommands.addCommand("export", variantCommandOptions.exportVariantCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_QUERY_COMMAND, variantCommandOptions.annotationQueryCommandOptions);
        variantSubCommands.addCommand(ANNOTATION_METADATA_COMMAND, variantCommandOptions.annotationMetadataCommandOptions);
        variantSubCommands.addCommand(STATS_RUN_COMMAND, variantCommandOptions.statsVariantCommandOptions);
        variantSubCommands.addCommand(SAMPLE_QUERY_COMMAND, variantCommandOptions.sampleQueryCommandOptions);
        variantSubCommands.addCommand(SAMPLE_RUN_COMMAND, variantCommandOptions.samplesFilterCommandOptions);
        variantSubCommands.addCommand(SAMPLE_VARIANT_STATS_RUN_COMMAND, variantCommandOptions.sampleVariantStatsCommandOptions);
        variantSubCommands.addCommand(COHORT_VARIANT_STATS_RUN_COMMAND, variantCommandOptions.cohortVariantStatsCommandOptions);
        variantSubCommands.addCommand(GWAS_RUN_COMMAND, variantCommandOptions.gwasCommandOptions);
        variantSubCommands.addCommand(PLINK_RUN_COMMAND, variantCommandOptions.plinkCommandOptions);
        variantSubCommands.addCommand(RVTEST_RUN_COMMAND, variantCommandOptions.rvtestsCommandOptions);

        operationsCommandOptions = new OperationsCommandOptions(this.commonCommandOptions, dataModelOptions, numericOptions, jCommander);
        jCommander.addCommand(OPERATIONS_COMMAND, operationsCommandOptions);
        JCommander operationsSubCommands = jCommander.getCommands().get(OPERATIONS_COMMAND);
        operationsSubCommands.addCommand(VARIANT_SECONDARY_INDEX, operationsCommandOptions.variantSecondaryIndex);
        operationsSubCommands.addCommand(VARIANT_SECONDARY_INDEX_DELETE, operationsCommandOptions.variantSecondaryIndexDelete);
        operationsSubCommands.addCommand(VARIANT_ANNOTATION_INDEX, operationsCommandOptions.variantAnnotation);
        operationsSubCommands.addCommand(VARIANT_ANNOTATION_SAVE, operationsCommandOptions.variantAnnotationSave);
        operationsSubCommands.addCommand(VARIANT_ANNOTATION_DELETE, operationsCommandOptions.variantAnnotationDelete);
        operationsSubCommands.addCommand(VARIANT_SCORE_INDEX, operationsCommandOptions.variantScoreIndex);
        operationsSubCommands.addCommand(VARIANT_SCORE_DELETE, operationsCommandOptions.variantScoreDelete);
        operationsSubCommands.addCommand(VARIANT_FAMILY_GENOTYPE_INDEX, operationsCommandOptions.variantFamilyIndex);
        operationsSubCommands.addCommand(VARIANT_SAMPLE_GENOTYPE_INDEX, operationsCommandOptions.variantSampleIndex);
        operationsSubCommands.addCommand(VARIANT_AGGREGATE, operationsCommandOptions.variantAggregate);
        operationsSubCommands.addCommand(VARIANT_FAMILY_AGGREGATE, operationsCommandOptions.variantAggregateFamily);
    }

    @Override
    public boolean isHelp() {
        String parsedCommand = jCommander.getParsedCommand();
        if (parsedCommand != null) {
            JCommander jCommander2 = jCommander.getCommands().get(parsedCommand);
            List<Object> objects = jCommander2.getObjects();
            if (!objects.isEmpty() && objects.get(0) instanceof AdminCliOptionsParser.AdminCommonCommandOptions) {
                return ((AdminCliOptionsParser.AdminCommonCommandOptions) objects.get(0)).help;
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
        Set<String> analysisCommands = new HashSet<>(Arrays.asList("alignments", "variant"));
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

    public VariableCommandOptions getVariableCommands() {
        return variableCommandOptions;
    }

    public CohortCommandOptions getCohortCommands() {
        return cohortCommandOptions;
    }

    public FamilyCommandOptions getFamilyCommands() {
        return familyCommandOptions;
    }

    public ClinicalCommandOptions getClinicalCommandOptions() {
        return clinicalCommandOptions;
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

    public OperationsCommandOptions getOperationsCommands() {
        return operationsCommandOptions;
    }
}
