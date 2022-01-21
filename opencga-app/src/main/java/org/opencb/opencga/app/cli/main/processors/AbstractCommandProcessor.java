package org.opencb.opencga.app.cli.main.processors;

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.executors.ExecutorProvider;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.parser.ParamParser;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import static org.opencb.commons.utils.PrintUtils.printWarn;

public abstract class AbstractCommandProcessor {

    private final ParamParser parser;

    public AbstractCommandProcessor(ParamParser parser) {
        this.parser = parser;
    }

    abstract void processCommand(OpencgaCommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser);

    public void process(String[] args) {
        OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();
        try {
            if (!ArrayUtils.isEmpty(args)) {
                //1. Parse params using specific parser because it does differently if it is the shell or the CLI
                parser.parseParams(args);
                if (args != null) {
                    //2. Parse params of options files
                    args = cliOptionsParser.parse(args);
                    if (!ArrayUtils.isEmpty(args)) {
                        CommandLineUtils.debug("PARSED OPTIONS ::: " + ArrayUtils.toString(args));
                        try {
                            // 3. Check if a command has been provided is valid
                            String parsedCommand = cliOptionsParser.getCommand();
                            CommandLineUtils.debug("COMMAND TO EXECUTE ::: " + ArrayUtils.toString(args));
                            if (cliOptionsParser.isValid(parsedCommand)) {
                                // 4. Get command executor from ExecutorProvider
                                CommandLineUtils.debug("COMMAND AND SUBCOMMAND ARE VALID");
                                OpencgaCommandExecutor commandExecutor = ExecutorProvider.getOpencgaCommandExecutor(cliOptionsParser, parsedCommand);
                                // 5. Execute parsed command with executor provided using CommandProcessor Implementation
                                CommandLineUtils.debug("EXECUTING ::: " + ArrayUtils.toString(args));
                                processCommand(commandExecutor, cliOptionsParser);
                            } else {
                                cliOptionsParser.printUsage();
                            }
                        } catch (ParameterException e) {
                            printWarn("\n" + e.getMessage());
                            cliOptionsParser.printUsage();
                        } catch (CatalogAuthenticationException e) {
                            printWarn("\n" + e.getMessage());
                        }
                    }
                }
            }
        } catch (Exception e) {
            CommandLineUtils.error(e);
            cliOptionsParser.printUsage();
        }

    }


}
