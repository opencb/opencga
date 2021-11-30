package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.ParameterException;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.main.executors.*;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.io.Console;
import java.io.IOException;

import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.ansi;

public class OpencgaCliProcessor {

    private static Console console;

    private static Console getConsole() {
        if (console == null) {
            console = System.console();
        }
        return console;
    }

    public static void process(String[] args) throws CatalogAuthenticationException {
        console = getConsole();
        OpencgaMain.printDebugMessage("Executing ", args);

        if (console == null && CliSessionManager.isShell()) {
            System.out.println("Couldn't get console instance");
            System.exit(0);
        }
        if (args.length == 1 && "exit".equals(args[0])) {
            OpencgaCliShellExecutor.printlnYellow("\nThanks for using OpenCGA. See you soon.\n\n");
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
            if (args.length > 3 && "users".equals(args[0]) && "login".equals(args[1]) && argsContains(args, "--user-password")) {
                if (!CliSessionManager.isShell()) {
                    args = getUserPasswordArgs(args, "--user-password");
                } else {
                    char[] passwordArray =
                            console.readPassword(String.valueOf(ansi().fg(GREEN).a("\nEnter your secret password: ").reset()));
                    args = appendArgs(args, new String[]{"--password", new String(passwordArray)});
                }
            }
        }
        OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();

        try {
            cliOptionsParser.parse(args);

            String parsedCommand = cliOptionsParser.getCommand();
            if (parsedCommand == null || parsedCommand.isEmpty()) {
                if (cliOptionsParser.getGeneralOptions().version) {

                    OpencgaCliShellExecutor.printGreen("\tOpenCGA CLI version: ");
                    OpencgaCliShellExecutor.printlnYellow("\t" + GitRepositoryState.get().getBuildVersion());
                    OpencgaCliShellExecutor.printGreen("\tGit version:");
                    OpencgaCliShellExecutor.printlnYellow("\t\t" + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId());
                    OpencgaCliShellExecutor.printGreen("\tProgram:");
                    OpencgaCliShellExecutor.printlnYellow("\t\tOpenCGA (OpenCB)");
                    OpencgaCliShellExecutor.printGreen("\tDescription: ");
                    OpencgaCliShellExecutor.printlnYellow("\t\tBig Data platform for processing and analysing NGS data");
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
                                OpencgaCliShellExecutor.printlnRed("ERROR: not valid command passed: '" + parsedCommand + "'");
                                break;
                        }

                        executeCommand(commandExecutor, cliOptionsParser);
                    }
                }
            }
        } catch (ParameterException e) {
            OpencgaMain.printWarningMessage("\n" + e.getMessage());
            cliOptionsParser.printUsage();
        } catch (CatalogAuthenticationException e) {
            OpencgaMain.printWarningMessage("\n" + e.getMessage());
            try {
                CliSessionManager.logoutCliSessionFile();
            } catch (IOException ex) {
                OpencgaMain.printErrorMessage(ex.getMessage(), ex);
            }
        }
    }

    private static boolean isNotHelpCommand(String[] args) {
        return !argsContains(args, "--help") && !argsContains(args, "-h");
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
            } catch (Exception e) {

                OpencgaMain.printErrorMessage(e.getMessage(), e);
            }
        } else {
            cliOptionsParser.printUsage();
        }
    }

    private static String[] appendArgs(String[] args, String[] strings) {
        String[] res = new String[args.length + strings.length];
        for (int i = 0; i < res.length; i++) {
            if (i < args.length) {
                res[i] = args[i];
            } else {
                res[i] = strings[i - args.length];
            }
        }
        return res;
    }

    private static boolean argsContains(String[] args, String s) {
        for (String arg : args) {
            if (arg.contains(s)) {
                return true;
            }
        }
        return false;
    }

    public static void forceLogin() throws CatalogAuthenticationException {
        console = getConsole();
        String user = console.readLine(String.valueOf(ansi().fg(GREEN).a("\nEnter your user: ").reset()));
        loginUser(user);
        OpencgaMain.printDebugMessage("Login user " + user);
    }

    public static void loginUser(String user) throws CatalogAuthenticationException {
        console = getConsole();
        char[] passwordArray = console.readPassword(String.valueOf(ansi().fg(GREEN).a("\nEnter your password: ").reset()));
        String[] args = new String[2];
        args[0] = "users";
        args[1] = "login";
        if (isValidUser(user)) {
            args = appendArgs(args, new String[]{"-u", user});
            args = appendArgs(args, new String[]{"--password", new String(passwordArray)});
            OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();
            cliOptionsParser.parse(args);
            CommandExecutor commandExecutor = new UsersCommandExecutor(cliOptionsParser.getUsersCommandOptions());
            executeCommand(commandExecutor, cliOptionsParser);
            OpencgaMain.printWarningMessage("Logged user " + user);
        } else {
            OpencgaMain.printWarningMessage("Invalid user name: " + user);
        }
    }

    private static boolean isValidUser(String user) {
        return user.matches("^[A-Za-z][A-Za-z0-9_]{2,29}$");
    }
}
