package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.main.executors.*;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.io.Console;
import java.io.IOException;

import static org.opencb.commons.utils.PrintUtils.*;


public class OpencgaCliProcessor {

    private static Console console;

    private static Console getConsole() {
        if (console == null) {
            console = System.console();
        }
        return console;
    }

    public static void execute(String[] args) throws CatalogAuthenticationException {
        console = getConsole();
        CommandLineUtils.printDebugMessage("Executing " + CommandLineUtils.getAsSpaceSeparatedString(args));


        if (console == null && CliSessionManager.isShell()) {
            println("Couldn't get console instance", Color.RED);
            System.exit(0);
        }
        if (args.length == 1 && "exit".equals(args[0])) {
            println("\nThanks for using OpenCGA. See you soon.\n\n", Color.YELLOW);
            System.exit(0);
        }

        if (args.length == 1 && "logout".equals(args[0])) {
            args = new String[]{"users", "logout"};
        }
        if (args.length == 1 && "login".equals(args[0])) {
            forceLogin();
            return;
        }

        if (args.length == 2 && "login".equals(args[0])) {
            loginUser(args[1]);
            return;
        }

        if (args.length == 3 && "use".equals(args[0]) && "study".equals(args[1])) {
            CliSessionManager.setValidatedCurrentStudy(args[2]);
            return;
        }

        if (args.length == 3 && "use".equals(args[0]) && "host".equals(args[1])) {
            if (CliSessionManager.DEFAULT_PARAMETER.equals(args[2])) {
                CliSessionManager.switchDefaultSessionHost();
            } else {
                CliSessionManager.switchSessionHost(args[2]);
            }
            return;
        }

        //login The first if clause is for scripting login method and the else clause is for the shell login
        if (isNotHelpCommand(args)) {
            if (args.length > 3 && "users".equals(args[0]) && "login".equals(args[1]) && ArrayUtils.contains(args, "--user-password")) {
                if (!CliSessionManager.isShell()) {
                    args = getUserPasswordArgs(args, "--user-password");
                } else {
                    char[] passwordArray =
                            console.readPassword(format("\nEnter your password: ", Color.GREEN));
                    args = ArrayUtils.addAll(args, "--password", new String(passwordArray));
                }
            }
        }
        OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();

        try {
            cliOptionsParser.parse(args);

            String parsedCommand = cliOptionsParser.getCommand();
            if (parsedCommand == null || parsedCommand.isEmpty()) {
                if (cliOptionsParser.getGeneralOptions().version) {
                    System.out.println(CommandLineUtils.getVersionString());
                    System.out.println();
                } else if (cliOptionsParser.getGeneralOptions().buildVersion) {
                    System.out.println(GitRepositoryState.get().getBuildVersion());
                } else if (cliOptionsParser.getGeneralOptions().help) {
                    cliOptionsParser.printUsage();
                } else {
                    cliOptionsParser.printUsage();
                }
            } else {
                CommandExecutor commandExecutor = null;
                // Check if any command -h option is present
                if (cliOptionsParser.isHelp()) {
                    cliOptionsParser.printUsage();
                    //System.exit(0);
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
                                CliSessionManager.setReloadStudies(true);
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
            }
        } catch (ParameterException e) {
            printWarn("\n" + e.getMessage());
            cliOptionsParser.printUsage();
        } catch (CatalogAuthenticationException e) {
            printWarn("\n" + e.getMessage());
            try {
                CliSessionManager.logoutCliSessionFile();
            } catch (IOException ex) {
                CommandLineUtils.printError("Failed to save OpenCGA CLI session", ex);

            }
        }
    }

    private static boolean isNotHelpCommand(String[] args) {
        return !ArrayUtils.contains(args, "--help") && !ArrayUtils.contains(args, "-h");
    }

    private static String[] getUserPasswordArgs(String[] args, String s) {
        for (int i = 0; i < args.length; i++) {
            if (s.equals(args[i])) {
                args[i] = "--password";
                break;
            }
        }
        return args;
    }

    private static void executeCommand(CommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser) {
        if (commandExecutor != null) {

            try {
                commandExecutor.execute();
                CliSessionManager.setDefaultCurrentStudy();
                CliSessionManager.updateSession();
            } catch (IOException e) {
                CommandLineUtils.printError("Could not set the default study", e);
            } catch (Exception ex) {
                CommandLineUtils.printError("Execution error", ex);
            }
        } else {
            cliOptionsParser.printUsage();
        }
    }

    private static void forceLogin() throws CatalogAuthenticationException {
        console = getConsole();
        String user = console.readLine(format("\nEnter your user: ", Color.GREEN));
        loginUser(user);
        CommandLineUtils.printDebugMessage("Login user " + user);

    }

    private static void loginUser(String user) throws CatalogAuthenticationException {
        console = getConsole();
        char[] passwordArray = console.readPassword(format("\nEnter your password: ", Color.GREEN));
        String[] args = new String[2];
        args[0] = "users";
        args[1] = "login";
        if (isValidUser(user)) {
            args = ArrayUtils.addAll(args, "-u", user);
            args = ArrayUtils.addAll(args, "--password", new String(passwordArray));
            OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();
            cliOptionsParser.parse(args);
            CommandExecutor commandExecutor = new UsersCommandExecutor(cliOptionsParser.getUsersCommandOptions());
            executeCommand(commandExecutor, cliOptionsParser);
            println(getKeyValueAsFormattedString("Logged user: ", user));
        } else {
            println(getKeyValueAsFormattedString("Invalid user name: ", user));
        }
    }

    @Deprecated
    private static boolean isValidUser(String user) {
        return user.matches("^[A-Za-z][A-Za-z0-9_]{2,29}$");
    }
}
