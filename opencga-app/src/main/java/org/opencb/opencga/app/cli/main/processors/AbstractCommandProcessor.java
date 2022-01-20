package org.opencb.opencga.app.cli.main.processors;

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.executors.ExecutorProvider;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.parser.ParamParser;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.main.utils.LoginUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.core.common.GitRepositoryState;

import static org.opencb.commons.utils.PrintUtils.printWarn;
import static org.opencb.commons.utils.PrintUtils.println;

public abstract class AbstractCommandProcessor {

    private final ParamParser parser;

    public AbstractCommandProcessor(ParamParser parser) {
        this.parser = parser;
    }

    abstract void processCommand(OpencgaCommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser);

    public void process(String[] args) {
        OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();
        try {
            //Process the shortcuts login, help, version, logout...
            args = processShortCuts(args, cliOptionsParser);
            if (!ArrayUtils.isEmpty(args)) {
                //Parse params using parser because it does differently if it is the shell or the CLI
                args = parser.parseParams(args);
                if (args != null) {
                    cliOptionsParser.parse(args);
                    CommandLineUtils.debug("PARSED OPTIONS ::: " + ArrayUtils.toString(args));
                    try {
                        // 1. Check if a command has been provided
                        String parsedCommand = cliOptionsParser.getCommand();
                        if (StringUtils.isEmpty(parsedCommand)) {
                            cliOptionsParser.printUsage();
                        } else {
                            // 2. Check if a subcommand has been provided
                            String parsedSubCommand = cliOptionsParser.getSubCommand();
                            if (StringUtils.isEmpty(parsedSubCommand)) {
                                cliOptionsParser.printUsage();
                            } else {
                                // 3. Get command executor from ExecutorProvider
                                OpencgaCommandExecutor commandExecutor = ExecutorProvider.getOpencgaCommandExecutor(cliOptionsParser, parsedCommand);
                                // 4. Execute command with parsed options using CommandProcessor implementation
                                processCommand(commandExecutor, cliOptionsParser);
                            }
                        }
                    } catch (ParameterException e) {
                        printWarn("\n" + e.getMessage());
                        cliOptionsParser.printUsage();
                    } catch (CatalogAuthenticationException e) {
                        printWarn("\n" + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            CommandLineUtils.error(e);
            cliOptionsParser.printUsage();
        }

    }

    public String[] processShortCuts(String[] args, OpencgaCliOptionsParser cliOptionsParser) throws CatalogAuthenticationException {
        switch (args[0]) {
            case "login":
                return LoginUtils.parseLoginCommand(args);
            case "--help":
            case "help":
            case "-h":
            case "?":
                cliOptionsParser.printUsage();
                return new String[0];
            case "--version":
            case "version":
                println(CommandLineUtils.getVersionString());
                return new String[0];
            case "--build-version":
            case "build-version":
                println(GitRepositoryState.get().getBuildVersion());
                return new String[0];
            case "logout":
                return ArrayUtils.addAll(new String[]{"users"}, args);
            default:
                return args;
        }
    }
}
