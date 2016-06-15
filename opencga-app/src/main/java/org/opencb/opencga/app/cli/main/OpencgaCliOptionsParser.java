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
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.app.cli.main.options.*;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.File;
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
        userSubCommands.addCommand("list", usersCommandOptions.listCommandOptions);
        userSubCommands.addCommand("login", usersCommandOptions.loginCommandOptions);
        userSubCommands.addCommand("logout", usersCommandOptions.logoutCommandOptions);

        projectCommandOptions = new ProjectCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("projects", projectCommandOptions);
        JCommander projectSubCommands = jCommander.getCommands().get("projects");
        projectSubCommands.addCommand("create", projectCommandOptions.createCommandOptions);
        projectSubCommands.addCommand("info", projectCommandOptions.infoCommandOptions);
        projectSubCommands.addCommand("studies", projectCommandOptions.studiesCommandOptions);
        projectSubCommands.addCommand("update", projectCommandOptions.updateCommandOptions);
        projectSubCommands.addCommand("delete", projectCommandOptions.deleteCommandOptions);

        studyCommandOptions = new StudyCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("studies", studyCommandOptions);
        JCommander studySubCommands = jCommander.getCommands().get("studies");
        studySubCommands.addCommand("create", studyCommandOptions.createCommandOptions);
        studySubCommands.addCommand("info", studyCommandOptions.infoCommandOptions);
        studySubCommands.addCommand("list", studyCommandOptions.listCommandOptions);
        studySubCommands.addCommand("resync", studyCommandOptions.resyncCommandOptions);
        studySubCommands.addCommand("check-files", studyCommandOptions.checkCommandOptions);
        studySubCommands.addCommand("status", studyCommandOptions.statusCommandOptions);
        studySubCommands.addCommand("annotate-variants", studyCommandOptions.annotationCommandOptions);

        fileCommandOptions = new FileCommandOptions(this.commonCommandOptions,jCommander);
        jCommander.addCommand("files", fileCommandOptions);
        JCommander fileSubCommands = jCommander.getCommands().get("files");
        fileSubCommands.addCommand("create", fileCommandOptions.createCommandOptions);
        fileSubCommands.addCommand("create-folder", fileCommandOptions.createFolderCommandOptions);
        fileSubCommands.addCommand("info", fileCommandOptions.infoCommandOptions);
        fileSubCommands.addCommand("download", fileCommandOptions.downloadCommandOptions);
        fileSubCommands.addCommand("grep", fileCommandOptions.grepCommandOptions);
        fileSubCommands.addCommand("search", fileCommandOptions.searchCommandOptions);
        fileSubCommands.addCommand("list", fileCommandOptions.listCommandOptions);
        fileSubCommands.addCommand("index", fileCommandOptions.indexCommandOptions);
        fileSubCommands.addCommand("alignaments", fileCommandOptions.alignamentsCommandOptions);
        fileSubCommands.addCommand("fetch", fileCommandOptions.fetchCommandOptions);
        fileSubCommands.addCommand("share", fileCommandOptions.shareCommandOptions);
        fileSubCommands.addCommand("unshare", fileCommandOptions.unshareCommandOptions);
        fileSubCommands.addCommand("update", fileCommandOptions.updateCommandOptions);
        fileSubCommands.addCommand("upload", fileCommandOptions.uploadCommandOptions);
        fileSubCommands.addCommand("link", fileCommandOptions.linkCommandOptions);
        fileSubCommands.addCommand("unlink", fileCommandOptions.unlinkCommandOptions);
        fileSubCommands.addCommand("relink", fileCommandOptions.relinkCommandOptions);
        fileSubCommands.addCommand("delete", fileCommandOptions.deleteCommandOptions);
        fileSubCommands.addCommand("refresh", fileCommandOptions.refreshCommandOptions);


        jobCommandOptions = new JobCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("jobs", jobCommandOptions);
        JCommander jobSubCommands = jCommander.getCommands().get("jobs");
        jobSubCommands.addCommand("create", jobCommandOptions.createCommandOptions);
        jobSubCommands.addCommand("info", jobCommandOptions.infoCommandOptions);
        jobSubCommands.addCommand("search", jobCommandOptions.searchCommandOptions);
        jobSubCommands.addCommand("visit", jobCommandOptions.visitCommandOptions);
        jobSubCommands.addCommand("delete", jobCommandOptions.deleteCommandOptions);
        jobSubCommands.addCommand("share", jobCommandOptions.shareCommandOptions);
        jobSubCommands.addCommand("unshare", jobCommandOptions.unshareCommandOptions);
        jobSubCommands.addCommand("group-by", jobCommandOptions.groupByCommandOptions);

       // jobSubCommands.addCommand("finished", jobCommandOptions.doneJobCommandOptions);
       // jobSubCommands.addCommand("status", jobCommandOptions.statusCommandOptions);
       // jobSubCommands.addCommand("run", jobCommandOptions.runJobCommandOptions);


        individualCommandOptions = new IndividualCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("individuals", individualCommandOptions);
        JCommander individualSubCommands = jCommander.getCommands().get("individuals");
        individualSubCommands.addCommand("create", individualCommandOptions.createCommandOptions);
        individualSubCommands.addCommand("info", individualCommandOptions.infoCommandOptions);
        individualSubCommands.addCommand("search", individualCommandOptions.searchCommandOptions);
        individualSubCommands.addCommand("annotate", individualCommandOptions.annotateCommandOptions);
        individualSubCommands.addCommand("update", individualCommandOptions.updateCommandOptions);
        individualSubCommands.addCommand("delete", individualCommandOptions.deleteCommandOptions);
        individualSubCommands.addCommand("share", individualCommandOptions.shareCommandOptions);
        individualSubCommands.addCommand("unshare", individualCommandOptions.unshareCommandOptions);
        individualSubCommands.addCommand("group-by", individualCommandOptions.groupByCommandOptions);

        sampleCommandOptions = new SampleCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("samples", sampleCommandOptions);
        JCommander sampleSubCommands = jCommander.getCommands().get("samples");
        sampleSubCommands.addCommand("create", sampleCommandOptions.createCommandOptions);
        sampleSubCommands.addCommand("load", sampleCommandOptions.loadCommandOptions);
        sampleSubCommands.addCommand("info", sampleCommandOptions.infoCommandOptions);
        sampleSubCommands.addCommand("search", sampleCommandOptions.searchCommandOptions);
        sampleSubCommands.addCommand("update", sampleCommandOptions.updateCommandOptions);
        sampleSubCommands.addCommand("delete", sampleCommandOptions.deleteCommandOptions);
        sampleSubCommands.addCommand("share", sampleCommandOptions.shareCommandOptions);
        sampleSubCommands.addCommand("unshare", sampleCommandOptions.unshareCommandOptions);
        sampleSubCommands.addCommand("group-by", sampleCommandOptions.groupByCommandOptions);
        sampleSubCommands.addCommand("annotate", sampleCommandOptions.annotateCommandOptions);

        variableCommandOptions = new VariableCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("variables", variableCommandOptions);
        JCommander variableSubCommands = jCommander.getCommands().get("variables");
        variableSubCommands.addCommand("create", variableCommandOptions.createCommandOptions);
        variableSubCommands.addCommand("info", variableCommandOptions.infoCommandOptions);
        variableSubCommands.addCommand("search", variableCommandOptions.searchCommandOptions);
        variableSubCommands.addCommand("delete", variableCommandOptions.deleteCommandOptions);
        variableSubCommands.addCommand("update", variableCommandOptions.updateCommandOptions);

        cohortCommandOptions = new CohortCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("cohorts", cohortCommandOptions);
        JCommander cohortSubCommands = jCommander.getCommands().get("cohorts");
        cohortSubCommands.addCommand("create", cohortCommandOptions.createCommandOptions);
        cohortSubCommands.addCommand("info", cohortCommandOptions.infoCommandOptions);
        cohortSubCommands.addCommand("samples", cohortCommandOptions.samplesCommandOptions);
        cohortSubCommands.addCommand("annotate", cohortCommandOptions.annotateCommandOptions);
        cohortSubCommands.addCommand("update", cohortCommandOptions.updateCommandOptions);
        cohortSubCommands.addCommand("delete", cohortCommandOptions.deleteCommandOptions);
        cohortSubCommands.addCommand("unshare", cohortCommandOptions.unshareCommandOptions);
        cohortSubCommands.addCommand("stats", cohortCommandOptions.statsCommandOptions);
        cohortSubCommands.addCommand("share", cohortCommandOptions.shareCommandOptions);
        cohortSubCommands.addCommand("group-by", cohortCommandOptions.groupByCommandOptions);

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
        panelSubCommands.addCommand("unshare", panelCommandOptions.unshareCommandOptions);
        panelSubCommands.addCommand("share", panelCommandOptions.shareCommandOptions);



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

    //    class CommonOptions {
    public static class OpencgaCommonCommandOptions extends GeneralCliOptions.CommonCommandOptions {

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        Map<String, String> dynamic = new HashMap<String, String>();

        @Parameter(names = {"--include"}, description = "", required = false, arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "", required = false, arity = 1)
        public String exclude;

        @Parameter(names = {"--metadata"}, description = "Include metadata information", required = false, arity = 1)
        public boolean metadata = false;

        @Parameter(names = {"--output-format"}, description = "Output format. one of {IDS, ID_CSV, NAME_ID_MAP, ID_LIST, RAW, PRETTY_JSON, PLAIN_JSON}", required = false, arity = 1)
        OutputFormat outputFormat = OutputFormat.PRETTY_JSON;

        QueryOptions getQueryOptions() {
            QueryOptions queryOptions = new QueryOptions(dynamic, false);
            if (include != null && !include.isEmpty()) {
                queryOptions.add("include", include);
            }
            if (exclude != null && !exclude.isEmpty()) {
                queryOptions.add("exclude", exclude);
            }
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

    public ToolCommandOptions getToolCommands() {  return toolCommandOptions; }




}
