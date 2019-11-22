package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;
import org.opencb.opencga.analysis.ToolAnalysis;
import org.opencb.opencga.app.cli.internal.options.ToolsCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.slf4j.LoggerFactory;

public class ToolsCommandExecutor extends InternalCommandExecutor {

    private final ToolsCommandOptions toolCommandOptions;

    public ToolsCommandExecutor(ToolsCommandOptions toolCommandOptions) {
        super(toolCommandOptions.commonCommandOptions);
        this.toolCommandOptions = toolCommandOptions;

        this.logger = LoggerFactory.getLogger(ToolsCommandExecutor.class);
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing tool command line");

        String subCommandString = getParsedSubCommand(toolCommandOptions.jCommander);
        configure();
        switch (subCommandString) {
            case "execute":
                executeTool();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void executeTool() {
        ToolsCommandOptions.ExecuteToolCommandOptions cliOptions = this.toolCommandOptions.executeToolCommandOptions;
        try {
            ToolAnalysis toolAnalysis = new ToolAnalysis(configuration);
            toolAnalysis.execute(Long.parseLong(cliOptions.job), cliOptions.commonOptions.sessionId);
        } catch (CatalogException | AnalysisToolException e) {
            logger.error("{}", e.getMessage(), e);
        }
    }
}
