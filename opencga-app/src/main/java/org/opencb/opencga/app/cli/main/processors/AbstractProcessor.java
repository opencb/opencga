package org.opencb.opencga.app.cli.main.processors;

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.executors.*;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.io.Console;
import java.io.IOException;

import static org.opencb.commons.utils.PrintUtils.*;


abstract class AbstractProcessor {

    protected Console console;

    private Console getConsole() {
        if (console == null) {
            console = System.console();
        }
        return console;
    }

    protected void process(OpencgaCliOptionsParser cliOptionsParser) {
        CommandExecutor commandExecutor = null;
        try {
            String parsedCommand = cliOptionsParser.getCommand();
            if (parsedCommand == null || parsedCommand.isEmpty()) {
                if (cliOptionsParser.getGeneralOptions().version) {
                    println(CommandLineUtils.getVersionString());
                } else if (cliOptionsParser.getGeneralOptions().buildVersion) {
                    println(GitRepositoryState.get().getBuildVersion());
                } else {
                    cliOptionsParser.printUsage();
                }
            } else {
                String parsedSubCommand = cliOptionsParser.getSubCommand();
                if (parsedSubCommand == null || parsedSubCommand.isEmpty()) {
                    cliOptionsParser.printUsage();
                } else {
                    switch (parsedCommand) {
                        case "users":
                            commandExecutor = new UsersCommandExecutor(cliOptionsParser.getUsersCommandOptions());
                            break;
                        case "projects":
                            commandExecutor = new ProjectsCommandExecutor(cliOptionsParser.getProjectsCommandOptions());
                            break;
                        case "studies":
                            commandExecutor = new StudiesCommandExecutor(cliOptionsParser.getStudiesCommandOptions());
                            break;
                        case "files":
                            commandExecutor = new FilesCommandExecutor(cliOptionsParser.getFilesCommandOptions());
                            break;
                        case "jobs":
                            commandExecutor = new JobsCommandExecutor(cliOptionsParser.getJobsCommandOptions());
                            break;
                        case "individuals":
                            commandExecutor = new IndividualsCommandExecutor(cliOptionsParser.getIndividualsCommandOptions());
                            break;
                        case "samples":
                            commandExecutor = new SamplesCommandExecutor(cliOptionsParser.getSamplesCommandOptions());
                            break;
                        case "cohorts":
                            commandExecutor = new CohortsCommandExecutor(cliOptionsParser.getCohortsCommandOptions());
                            break;
                        case "panels":
                            commandExecutor = new DiseasePanelsCommandExecutor(cliOptionsParser.getDiseasePanelsCommandOptions());
                            break;
                        case "families":
                            commandExecutor = new FamiliesCommandExecutor(cliOptionsParser.getFamiliesCommandOptions());
                            break;
                        case "alignments":
                            commandExecutor =
                                    new AnalysisAlignmentCommandExecutor(cliOptionsParser.getAnalysisAlignmentCommandOptions());
                            break;
                        case "variant":
                            commandExecutor =
                                    new AnalysisVariantCommandExecutor(cliOptionsParser.getAnalysisVariantCommandOptions());
                            break;
                        case "clinical":
                            commandExecutor =
                                    new AnalysisClinicalCommandExecutor(cliOptionsParser.getAnalysisClinicalCommandOptions());
                            break;
                        case "operations":
                            commandExecutor =
                                    new OperationsVariantStorageCommandExecutor(cliOptionsParser.getOperationsVariantStorageCommandOptions());
                            break;
                        case "meta":
                            commandExecutor = new MetaCommandExecutor(cliOptionsParser.getMetaCommandOptions());
                            break;
                        default:
                            printError("Not valid command passed: '" + parsedCommand + "'");
                            break;
                    }
                    executeCommand(commandExecutor, cliOptionsParser);
                }

            }
        } catch (ParameterException e) {
            printWarn("\n" + e.getMessage());
            cliOptionsParser.printUsage();
        } catch (CatalogAuthenticationException e) {
            printWarn("\n" + e.getMessage());
            try {
                CliSessionManager.getInstance().logoutCliSessionFile();
            } catch (IOException | ClientException ex) {
                CommandLineUtils.printError("Failed to save OpenCGA CLI session", ex);
            }
        }
    }

    private void executeCommand(CommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser) {
        if (commandExecutor != null) {
            try {
                commandExecutor.execute();
                CliSessionManager.getInstance().updateSession();
            } catch (IOException e) {
                CommandLineUtils.printError("Could not set the default study", e);
            } catch (Exception ex) {
                CommandLineUtils.printError("Execution error", ex);
            }
        } else {
            cliOptionsParser.printUsage();
        }
    }

    protected void forceLogin(String[] args) throws CatalogAuthenticationException {
        console = getConsole();
        String user = console.readLine(format("\nEnter your user: ", Color.GREEN));
        loginUser(args, user);
        CommandLineUtils.printDebug("Login user " + user);

    }

    protected void loginUser(String[] args, String user) throws CatalogAuthenticationException {
        console = getConsole();
        char[] passwordArray = console.readPassword(format("\nEnter your password: ", Color.GREEN));
        if (CommandLineUtils.isValidUser(user)) {
            args = ArrayUtils.addAll(args, "-u", user);
            args = ArrayUtils.addAll(args, "--password", new String(passwordArray));
            CommandLineUtils.printDebug(ArrayUtils.toString(args));
            OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();
            cliOptionsParser.parse(args);
            CommandExecutor commandExecutor = new UsersCommandExecutor(cliOptionsParser.getUsersCommandOptions());
            executeCommand(commandExecutor, cliOptionsParser);
            println(getKeyValueAsFormattedString("Logged user: ", user));
        } else {
            println(getKeyValueAsFormattedString("Invalid user name: ", user));
        }
    }

    public void execute(String[] args) throws CatalogAuthenticationException {
        console = getConsole();
        if (!isLoginCommand(args)) {
            if (ArrayUtils.contains(args, "logout")) {
                args = normaliceCLIUsersArgs(args);
            } else {
                parseParams(args);
            }
            OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();
            cliOptionsParser.parse(args);
            if (cliOptionsParser.isHelp()) {
                cliOptionsParser.printUsage();
            } else {
                process(cliOptionsParser);
            }
        }
    }

    private boolean isLoginCommand(String[] consoleArgs) throws CatalogAuthenticationException {
        if (ArrayUtils.contains(consoleArgs, "login")) {
            String[] args = normaliceCLIUsersArgs(consoleArgs);
            if (consoleArgs.length == 1 && "login".equals(consoleArgs[0])) {
                forceLogin(args);
                return true;
            }
            if (consoleArgs.length > 1 && "login".equals(consoleArgs[0])) {
                if (consoleArgs[1].equals("--host")) {
                    forceLogin(args);
                } else {
                    args = ArrayUtils.remove(args, 2);
                    loginUser(args, consoleArgs[1]);
                }
                return true;
            }

        }
        return false;
    }

    private String[] normaliceCLIUsersArgs(String[] consoleArgs) {
        String[] args = new String[consoleArgs.length + 1];
        args[0] = "users";
        for (int i = 1; i < args.length; i++) {
            args[i] = consoleArgs[i - 1];
        }
        return args;
    }


    protected boolean isNotHelpCommand(String[] args) {
        return !ArrayUtils.contains(args, "--help") && !ArrayUtils.contains(args, "-h");
    }

    abstract void parseParams(String[] args) throws CatalogAuthenticationException;
}
