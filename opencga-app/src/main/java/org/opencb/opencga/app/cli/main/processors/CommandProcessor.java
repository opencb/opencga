package org.opencb.opencga.app.cli.main.processors;

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.executors.ExecutorProvider;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.parser.ParamParser;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import static org.opencb.commons.utils.PrintUtils.printWarn;

public abstract class CommandProcessor extends Processor {


    public CommandProcessor(ParamParser parser) {
        super(parser);
    }

    abstract void processCommand(OpencgaCommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser);

    protected void processCommandOptions(OpencgaCliOptionsParser cliOptionsParser) {

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
                    // 3. Create the command executor
                    OpencgaCommandExecutor commandExecutor = ExecutorProvider.getOpencgaCommandExecutor(cliOptionsParser, parsedCommand);
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
