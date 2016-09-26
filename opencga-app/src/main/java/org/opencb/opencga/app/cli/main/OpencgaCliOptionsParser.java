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

package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.*;
import com.beust.jcommander.converters.IParameterSplitter;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.app.cli.main.options.catalog.*;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on AdminMain.
 */
public class OpencgaCliOptionsParser {

    private final JCommander jCommander;

    private final GeneralOptions generalOptions;
    private final OpencgaCommonCommandOptions commonCommandOptions;

    protected final UserAndPasswordOptions userAndPasswordOptions;

    private UserCommandOptions usersCommandOptions;
    private ProjectCommandOptions projectCommandOptions;
    private StudyCommandOptions studyCommandOptions;
    private FileCommandOptions fileCommandOptions;
    private JobCommandOptions jobCommandOptions;
    private IndividualCommandOptions individualCommandOptions;
    private SampleCommandOptions sampleCommandOptions;
    private VariableCommandOptions variableCommandOptions;
    private CohortCommandOptions cohortCommandOptions;
    private PanelCommandOptions panelCommandOptions;
    private ToolCommandOptions toolCommandOptions;


//    public final CommandShareResource commandShareResource;

    public OpencgaCliOptionsParser() {
        this(false);
    }

    public OpencgaCliOptionsParser(boolean interactive) {
        generalOptions = new GeneralOptions();

        jCommander = new JCommander(generalOptions);

        commonCommandOptions = new OpencgaCommonCommandOptions();
        userAndPasswordOptions = new UserAndPasswordOptions();
//        commandShareResource = new CommandShareResource();

        usersCommandOptions = new UserCommandOptions(this.commonCommandOptions, this.jCommander);
        jCommander.addCommand("users", usersCommandOptions);
        JCommander userSubCommands = jCommander.getCommands().get("users");
        userSubCommands.addCommand("create", usersCommandOptions.createCommandOptions);
        userSubCommands.addCommand("info", usersCommandOptions.infoCommandOptions);
        userSubCommands.addCommand("update", usersCommandOptions.updateCommandOptions);
        userSubCommands.addCommand("change-password", usersCommandOptions.changePaswordCommandOptions);
        userSubCommands.addCommand("delete", usersCommandOptions.deleteCommandOptions);
        userSubCommands.addCommand("projects", usersCommandOptions.projectsCommandOptions);
        userSubCommands.addCommand("login", usersCommandOptions.loginCommandOptions);
        userSubCommands.addCommand("logout", usersCommandOptions.logoutCommandOptions);
        userSubCommands.addCommand("reset-password", usersCommandOptions.resetPasswordCommandOptions);
        // TODO: "password" -> Allow to change

        projectCommandOptions = new ProjectCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("projects", projectCommandOptions);
        JCommander projectSubCommands = jCommander.getCommands().get("projects");
        projectSubCommands.addCommand("create", projectCommandOptions.createCommandOptions);
        projectSubCommands.addCommand("info", projectCommandOptions.infoCommandOptions);
        projectSubCommands.addCommand("studies", projectCommandOptions.studiesCommandOptions);
        projectSubCommands.addCommand("update", projectCommandOptions.updateCommandOptions);
        projectSubCommands.addCommand("delete", projectCommandOptions.deleteCommandOptions);
        projectSubCommands.addCommand("help", projectCommandOptions.helpCommandOptions);

        studyCommandOptions = new StudyCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("studies", studyCommandOptions);
        JCommander studySubCommands = jCommander.getCommands().get("studies");
        studySubCommands.addCommand("create", studyCommandOptions.createCommandOptions);
        studySubCommands.addCommand("info", studyCommandOptions.infoCommandOptions);
        studySubCommands.addCommand("search", studyCommandOptions.searchCommandOptions);
        studySubCommands.addCommand("summary", studyCommandOptions.summaryCommandOptions);
        studySubCommands.addCommand("delete", studyCommandOptions.deleteCommandOptions);
        studySubCommands.addCommand("update", studyCommandOptions.updateCommandOptions);
        studySubCommands.addCommand("scan-files", studyCommandOptions.scanFilesCommandOptions);
        studySubCommands.addCommand("files", studyCommandOptions.filesCommandOptions);
        studySubCommands.addCommand("jobs", studyCommandOptions.jobsCommandOptions);
        studySubCommands.addCommand("alignments", studyCommandOptions.alignmentsCommandOptions);
        studySubCommands.addCommand("samples", studyCommandOptions.samplesCommandOptions);
        studySubCommands.addCommand("variants", studyCommandOptions.variantsCommandOptions);
        studySubCommands.addCommand("help", studyCommandOptions.helpCommandOptions);
        studySubCommands.addCommand("groups", studyCommandOptions.groupsCommandOptions);
        studySubCommands.addCommand("groups-create", studyCommandOptions.groupsCreateCommandOptions);
        studySubCommands.addCommand("groups-delete", studyCommandOptions.groupsDeleteCommandOptions);
        studySubCommands.addCommand("groups-info", studyCommandOptions.groupsInfoCommandOptions);
        studySubCommands.addCommand("groups-update", studyCommandOptions.groupsUpdateCommandOptions);
        studySubCommands.addCommand("acl", studyCommandOptions.aclsCommandOptions);
        studySubCommands.addCommand("acl-create", studyCommandOptions.aclsCreateCommandOptions);
        studySubCommands.addCommand("acl-member-delete", studyCommandOptions.aclsMemberDeleteCommandOptions);
        studySubCommands.addCommand("acl-member-info", studyCommandOptions.aclsMemberInfoCommandOptions);
        studySubCommands.addCommand("acl-member-update", studyCommandOptions.aclsMemberUpdateCommandOptions);

        fileCommandOptions = new FileCommandOptions(this.commonCommandOptions,jCommander);
        jCommander.addCommand("files", fileCommandOptions);
        JCommander fileSubCommands = jCommander.getCommands().get("files");
        fileSubCommands.addCommand("copy", fileCommandOptions.copyCommandOptions);
        fileSubCommands.addCommand("create-folder", fileCommandOptions.createFolderCommandOptions);
        fileSubCommands.addCommand("info", fileCommandOptions.infoCommandOptions);
        fileSubCommands.addCommand("download", fileCommandOptions.downloadCommandOptions);
        fileSubCommands.addCommand("grep", fileCommandOptions.grepCommandOptions);
        fileSubCommands.addCommand("search", fileCommandOptions.searchCommandOptions);
        fileSubCommands.addCommand("list", fileCommandOptions.listCommandOptions);
        fileSubCommands.addCommand("index", fileCommandOptions.indexCommandOptions);
        fileSubCommands.addCommand("alignment", fileCommandOptions.alignmentCommandOptions);
        fileSubCommands.addCommand("content", fileCommandOptions.contentCommandOptions);
//        fileSubCommands.addCommand("fetch", fileCommandOptions.fetchCommandOptions);
        fileSubCommands.addCommand("update", fileCommandOptions.updateCommandOptions);
        fileSubCommands.addCommand("upload", fileCommandOptions.uploadCommandOptions);
        fileSubCommands.addCommand("link", fileCommandOptions.linkCommandOptions);
        fileSubCommands.addCommand("unlink", fileCommandOptions.unlinkCommandOptions);
        fileSubCommands.addCommand("relink", fileCommandOptions.relinkCommandOptions);
        fileSubCommands.addCommand("delete", fileCommandOptions.deleteCommandOptions);
        fileSubCommands.addCommand("refresh", fileCommandOptions.refreshCommandOptions);
        fileSubCommands.addCommand("variants", fileCommandOptions.variantsCommandOptions);
        fileSubCommands.addCommand("acl", fileCommandOptions.aclsCommandOptions);
        fileSubCommands.addCommand("acl-create", fileCommandOptions.aclsCreateCommandOptions);
        fileSubCommands.addCommand("acl-member-delete", fileCommandOptions.aclsMemberDeleteCommandOptions);
        fileSubCommands.addCommand("acl-member-info", fileCommandOptions.aclsMemberInfoCommandOptions);
        fileSubCommands.addCommand("acl-member-update", fileCommandOptions.aclsMemberUpdateCommandOptions);


        jobCommandOptions = new JobCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("jobs", jobCommandOptions);
        JCommander jobSubCommands = jCommander.getCommands().get("jobs");
        jobSubCommands.addCommand("create", jobCommandOptions.createCommandOptions);
        jobSubCommands.addCommand("info", jobCommandOptions.infoCommandOptions);
        jobSubCommands.addCommand("search", jobCommandOptions.searchCommandOptions);
        jobSubCommands.addCommand("visit", jobCommandOptions.visitCommandOptions);
        jobSubCommands.addCommand("delete", jobCommandOptions.deleteCommandOptions);
        jobSubCommands.addCommand("group-by", jobCommandOptions.groupByCommandOptions);
        jobSubCommands.addCommand("acl", jobCommandOptions.aclsCommandOptions);
        jobSubCommands.addCommand("acl-create", jobCommandOptions.aclsCreateCommandOptions);
        jobSubCommands.addCommand("acl-member-delete", jobCommandOptions.aclsMemberDeleteCommandOptions);
        jobSubCommands.addCommand("acl-member-info", jobCommandOptions.aclsMemberInfoCommandOptions);
        jobSubCommands.addCommand("acl-member-update", jobCommandOptions.aclsMemberUpdateCommandOptions);

       // jobSubCommands.addCommand("finished", jobCommandOptions.doneJobCommandOptions);
       // jobSubCommands.addCommand("status", jobCommandOptions.statusCommandOptions);
       // jobSubCommands.addCommand("run", jobCommandOptions.runJobCommandOptions);


        individualCommandOptions = new IndividualCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("individuals", individualCommandOptions);
        JCommander individualSubCommands = jCommander.getCommands().get("individuals");
        individualSubCommands.addCommand("create", individualCommandOptions.createCommandOptions);
        individualSubCommands.addCommand("info", individualCommandOptions.infoCommandOptions);
        individualSubCommands.addCommand("search", individualCommandOptions.searchCommandOptions);
        individualSubCommands.addCommand("update", individualCommandOptions.updateCommandOptions);
        individualSubCommands.addCommand("delete", individualCommandOptions.deleteCommandOptions);
        individualSubCommands.addCommand("group-by", individualCommandOptions.groupByCommandOptions);
        individualSubCommands.addCommand("acl", individualCommandOptions.aclsCommandOptions);
        individualSubCommands.addCommand("acl-create", individualCommandOptions.aclsCreateCommandOptions);
        individualSubCommands.addCommand("acl-member-delete", individualCommandOptions.aclsMemberDeleteCommandOptions);
        individualSubCommands.addCommand("acl-member-info", individualCommandOptions.aclsMemberInfoCommandOptions);
        individualSubCommands.addCommand("acl-member-update", individualCommandOptions.aclsMemberUpdateCommandOptions);
        individualSubCommands.addCommand("annotation-sets-create", individualCommandOptions.annotationCreateCommandOptions);
        individualSubCommands.addCommand("annotation-sets-all-info", individualCommandOptions.annotationAllInfoCommandOptions);
        individualSubCommands.addCommand("annotation-sets-info", individualCommandOptions.annotationInfoCommandOptions);
        individualSubCommands.addCommand("annotation-sets-search", individualCommandOptions.annotationSearchCommandOptions);
        individualSubCommands.addCommand("annotation-sets-update", individualCommandOptions.annotationUpdateCommandOptions);
        individualSubCommands.addCommand("annotation-sets-delete", individualCommandOptions.annotationDeleteCommandOptions);

        sampleCommandOptions = new SampleCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("samples", sampleCommandOptions);
        JCommander sampleSubCommands = jCommander.getCommands().get("samples");
        sampleSubCommands.addCommand("create", sampleCommandOptions.createCommandOptions);
        sampleSubCommands.addCommand("load", sampleCommandOptions.loadCommandOptions);
        sampleSubCommands.addCommand("info", sampleCommandOptions.infoCommandOptions);
        sampleSubCommands.addCommand("search", sampleCommandOptions.searchCommandOptions);
        sampleSubCommands.addCommand("update", sampleCommandOptions.updateCommandOptions);
        sampleSubCommands.addCommand("delete", sampleCommandOptions.deleteCommandOptions);
        sampleSubCommands.addCommand("group-by", sampleCommandOptions.groupByCommandOptions);
        sampleSubCommands.addCommand("acl", sampleCommandOptions.aclsCommandOptions);
        sampleSubCommands.addCommand("acl-create", sampleCommandOptions.aclsCreateCommandOptions);
        sampleSubCommands.addCommand("acl-member-delete", sampleCommandOptions.aclsMemberDeleteCommandOptions);
        sampleSubCommands.addCommand("acl-member-info", sampleCommandOptions.aclsMemberInfoCommandOptions);
        sampleSubCommands.addCommand("acl-member-update", sampleCommandOptions.aclsMemberUpdateCommandOptions);
        sampleSubCommands.addCommand("annotation-sets-create", sampleCommandOptions.annotationCreateCommandOptions);
        sampleSubCommands.addCommand("annotation-sets-all-info", sampleCommandOptions.annotationAllInfoCommandOptions);
        sampleSubCommands.addCommand("annotation-sets-info", sampleCommandOptions.annotationInfoCommandOptions);
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
        variableSubCommands.addCommand("update", variableCommandOptions.updateCommandOptions);
        variableSubCommands.addCommand("field-add", variableCommandOptions.fieldAddCommandOptions);
        variableSubCommands.addCommand("field-delete", variableCommandOptions.fieldDeleteCommandOptions);
        variableSubCommands.addCommand("field-rename", variableCommandOptions.fieldRenameCommandOptions);

        cohortCommandOptions = new CohortCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("cohorts", cohortCommandOptions);
        JCommander cohortSubCommands = jCommander.getCommands().get("cohorts");
        cohortSubCommands.addCommand("create", cohortCommandOptions.createCommandOptions);
        cohortSubCommands.addCommand("info", cohortCommandOptions.infoCommandOptions);
        cohortSubCommands.addCommand("samples", cohortCommandOptions.samplesCommandOptions);
        cohortSubCommands.addCommand("update", cohortCommandOptions.updateCommandOptions);
        cohortSubCommands.addCommand("delete", cohortCommandOptions.deleteCommandOptions);
        cohortSubCommands.addCommand("stats", cohortCommandOptions.statsCommandOptions);
        cohortSubCommands.addCommand("group-by", cohortCommandOptions.groupByCommandOptions);
        cohortSubCommands.addCommand("acl", cohortCommandOptions.aclsCommandOptions);
        cohortSubCommands.addCommand("acl-create", cohortCommandOptions.aclsCreateCommandOptions);
        cohortSubCommands.addCommand("acl-member-delete", cohortCommandOptions.aclsMemberDeleteCommandOptions);
        cohortSubCommands.addCommand("acl-member-info", cohortCommandOptions.aclsMemberInfoCommandOptions);
        cohortSubCommands.addCommand("acl-member-update", cohortCommandOptions.aclsMemberUpdateCommandOptions);
        cohortSubCommands.addCommand("annotation-sets-create", cohortCommandOptions.annotationCreateCommandOptions);
        cohortSubCommands.addCommand("annotation-sets-all-info", cohortCommandOptions.annotationAllInfoCommandOptions);
        cohortSubCommands.addCommand("annotation-sets-info", cohortCommandOptions.annotationInfoCommandOptions);
        cohortSubCommands.addCommand("annotation-sets-search", cohortCommandOptions.annotationSearchCommandOptions);
        cohortSubCommands.addCommand("annotation-sets-update", cohortCommandOptions.annotationUpdateCommandOptions);
        cohortSubCommands.addCommand("annotation-sets-delete", cohortCommandOptions.annotationDeleteCommandOptions);


        toolCommandOptions = new ToolCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("tools", toolCommandOptions);
        JCommander toolSubCommands = jCommander.getCommands().get("tools");
        toolSubCommands.addCommand("help", toolCommandOptions.helpCommandOptions);
        toolSubCommands.addCommand("info", toolCommandOptions.infoCommandOptions);
        toolSubCommands.addCommand("search", toolCommandOptions.searchCommandOptions);
        toolSubCommands.addCommand("update", toolCommandOptions.updateCommandOptions);
        toolSubCommands.addCommand("delete", toolCommandOptions.deleteCommandOptions);

        panelCommandOptions = new PanelCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("panels", panelCommandOptions);
        JCommander panelSubCommands = jCommander.getCommands().get("panels");
        panelSubCommands.addCommand("create", panelCommandOptions.createCommandOptions);
        panelSubCommands.addCommand("info", panelCommandOptions.infoCommandOptions);
        panelSubCommands.addCommand("acl", panelCommandOptions.aclsCommandOptions);
        panelSubCommands.addCommand("acl-create", panelCommandOptions.aclsCreateCommandOptions);
        panelSubCommands.addCommand("acl-member-delete", panelCommandOptions.aclsMemberDeleteCommandOptions);
        panelSubCommands.addCommand("acl-member-info", panelCommandOptions.aclsMemberInfoCommandOptions);
        panelSubCommands.addCommand("acl-member-update", panelCommandOptions.aclsMemberUpdateCommandOptions);


        if (interactive) { //Add interactive commands
//            jCommander.addCommand(new HelpCommands());
            jCommander.addCommand(new ExitCommands());
        }
    }


//    public void printUsage(){
//        if(!getCommand().isEmpty()) {
//            if(!getSubCommand().isEmpty()){
////                usage(getCommand(), getSubcommand());
//                jCommander.getCommands().get(getCommand()).usage(getSubCommand());
//            } else {
////                jCommander.usage(getCommand());
//                new JCommander(jCommander.getCommands().get(getCommand()).getObjects().get(0)).usage();
//                System.err.println("Available commands");
//                printUsage(jCommander.getCommands().get(getCommand()));
//            }
//        } else {
//            new JCommander(generalOptions).usage();
//            System.err.println("Available commands");
//            printUsage(jCommander);
//        }
//    }

//    private void printUsage(JCommander commander) {
//        int gap = 10;
//        int leftGap = 1;
//        for (String s : commander.getCommands().keySet()) {
//            if (gap < s.length() + leftGap) {
//                gap = s.length() + leftGap;
//            }
//        }
//        for (Map.Entry<String, JCommander> entry : commander.getCommands().entrySet()) {
//            System.err.printf("%" + gap + "s    %s\n", entry.getKey(), commander.getCommandDescription(entry.getKey()));
//        }
//    }

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
            if (!objects.isEmpty() && objects.get(0) instanceof AdminCliOptionsParser.AdminCommonCommandOptions) {
                return ((AdminCliOptionsParser.AdminCommonCommandOptions) objects.get(0)).help;
            }
        }
        return commonCommandOptions.help;
    }




    public class GeneralOptions {
        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"-V", "--version"})
        public boolean version;

        @Parameter(names = {"-i", "--interactive"})
        public boolean interactive;
    }

    /**
     * This class contains all those parameters available for all 'commands'
     */
    public class CommandOptions {

//        @Parameter(names = {"-h", "--help"},  description = "This parameter prints this help", help = true)
//        public boolean help;

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

    public class UserAndPasswordOptions {

        @Parameter(names = {"-u", "--user"}, description = "UserId", required = false, arity = 1)
        public String user;

        @Parameter(names = {"-p", "--password"}, description = "Password", arity = 1, required = false,  password = true)
        public String password;

        @Deprecated
        @Parameter(names = {"-hp", "--hidden-password"}, description = "Password", arity = 1, required = false,  password = true)
        public String hiddenPassword;

        @Parameter(names = {"-sid", "--session-id"}, description = "SessionId", arity = 1, required = false, hidden = true)
        public String sessionId;
    }

    enum OutputFormat {IDS, ID_CSV, NAME_ID_MAP, ID_LIST, RAW, PRETTY_JSON, PLAIN_JSON}

    public static class OpencgaCommonCommandOptions extends GeneralCliOptions.CommonCommandOptions {

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        Map<String, String> dynamic = new HashMap<>();

        @Parameter(names = {"--metadata"}, description = "Include metadata information", required = false, arity = 1)
        public boolean metadata = false;

        @Parameter(names = {"--output-format"}, description = "Output format. one of {IDS, ID_CSV, NAME_ID_MAP, ID_LIST, RAW, PRETTY_JSON, "
                + "PLAIN_JSON}", required = false, arity = 1)
        OutputFormat outputFormat = OutputFormat.PRETTY_JSON;

    }

    @Deprecated
    public static class OpencgaIncludeExcludeCommonCommandOptions extends OpencgaCommonCommandOptions {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        QueryOptions getQueryOptions() {
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.putIfNotNull("include", include);
            queryOptions.putIfNotNull("exclude", exclude);
            return queryOptions;
        }

    }

    @Deprecated
    public static class OpencgaQueryOptionsCommonCommandOptions extends OpencgaIncludeExcludeCommonCommandOptions {

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Override
        QueryOptions getQueryOptions() {
            QueryOptions queryOptions = super.getQueryOptions();
            queryOptions.putIfNotNull("skip", skip);
            queryOptions.putIfNotNull("limit", limit);
            return queryOptions;
        }
    }


    class BasicCommand {
    }

    @Parameters(commandNames = {"help"}, commandDescription = "Description")
    class HelpCommands {
    }

    @Parameters(commandNames = {"exit"}, commandDescription = "Description")
    class ExitCommands {
    }



    public static class NoSplitter implements IParameterSplitter {

        public List<String> split(String value) {
            return Arrays.asList(value.split(";"));
        }

    }


    @Parameters(commandNames = {"share"}, commandDescription = "Share resource")
    class CommandShareResource {
        @ParametersDelegate
        UserAndPasswordOptions up = userAndPasswordOptions;

        @ParametersDelegate
        OpencgaCommonCommandOptions cOpt = commonCommandOptions;

        @Parameter(names = {"-id"}, description = "Unique identifier", required = true, arity = 1)
        public String id;

        @Parameter(names = {"-U"}, description = "User to share", required = true, arity = 1)
        public String user;

        @Parameter(names = {"-r"}, description = "Read", required = true, arity = 1)
        public boolean read;

        @Parameter(names = {"-w"}, description = "Write", required = true, arity = 1)
        public boolean write;

        @Parameter(names = {"-x"}, description = "Execute", required = true, arity = 1)
        public boolean execute;

        @Parameter(names = {"-d"}, description = "Delete", required = true, arity = 1)
        public boolean delete;
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
            System.err.println("Usage:       opencga.sh [-h|--help] [--version] <command> [options]");
            System.err.println("");
            System.err.println("Commands:");
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

    public GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    public OpencgaCommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }

    public UserAndPasswordOptions getUserAndPasswordOptions() {
        return userAndPasswordOptions;
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

    public PanelCommandOptions getPanelCommands() {
        return panelCommandOptions;
    }

    public ToolCommandOptions getToolCommands() {
        return toolCommandOptions;
    }

}
