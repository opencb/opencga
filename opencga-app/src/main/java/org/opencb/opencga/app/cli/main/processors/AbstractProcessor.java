package org.opencb.opencga.app.cli.main.processors;

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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


public abstract class AbstractProcessor {

    protected Console console;

    protected AbstractProcessor() {
        this.console = getConsole();
    }

    abstract void parseParams(String[] args) throws CatalogAuthenticationException;

    private Console getConsole() {
        if (console == null) {
            console = System.console();
        }
        return console;
    }


    protected void process(OpencgaCliOptionsParser cliOptionsParser) {
        CommandExecutor commandExecutor = null;
        try {
            // 1. Check if a command has been provided
            String parsedCommand = cliOptionsParser.getCommand();
            if (StringUtils.isEmpty(parsedCommand)) {
                if (cliOptionsParser.getGeneralOptions().version) {
                    println(CommandLineUtils.getVersionString());
                    System.exit(0);
                } else if (cliOptionsParser.getGeneralOptions().buildVersion) {
                    println(GitRepositoryState.get().getBuildVersion());
                    System.exit(0);
                } else {
                    cliOptionsParser.printUsage();
                    System.exit(0);
                }
            } else {
                // 2. Check if a subcommand has been provided
                String parsedSubCommand = cliOptionsParser.getSubCommand();
                if (StringUtils.isEmpty(parsedSubCommand)) {
                    cliOptionsParser.printUsage();
                    System.exit(0);
                } else {
                    // 3. Create the command executor
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
                System.exit(1);
            } catch (Exception ex) {
                CommandLineUtils.printError("Execution error", ex);
                System.exit(1);
            }
        } else {
            cliOptionsParser.printUsage();
            System.exit(1);
        }
    }

    protected void forceLogin(String[] args) throws CatalogAuthenticationException {
        String user = getConsole().readLine(format("\nEnter your user: ", Color.GREEN));
        loginUser(args, user);
        CommandLineUtils.printDebug("Login user " + user);
    }

    protected void loginUser(String[] args, String user) throws CatalogAuthenticationException {
        char[] passwordArray = getConsole().readPassword(format("\nEnter your password: ", Color.GREEN));
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
        if (!isLoginCommand(args)) {
            if (ArrayUtils.contains(args, "logout")) {
                args = normalizeCLIUsersArgs(args);
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
            //adds in position 0 command "users"
            String[] args = normalizeCLIUsersArgs(consoleArgs);
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


    private String[] normalizeCLIUsersArgs(String[] consoleArgs) {
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
}
