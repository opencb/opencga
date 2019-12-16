package org.opencb.opencga.app.cli.internal.executors;

import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.tools.ToolFactory;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.app.cli.internal.options.ToolsCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.exception.ToolException;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Collection;

public class ToolsCommandExecutor extends InternalCommandExecutor {

    private final ToolsCommandOptions toolCommandOptions;
    private ToolRunner toolRunner;

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
        toolRunner = new ToolRunner(appHome, catalogManager, storageEngineFactory);
        switch (subCommandString) {
            case "execute-tool":
                executeTool();
                break;
            case "execute-job":
                executeJob();
                break;
            case "list":
                listTools();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void executeTool() throws ToolException {
        ToolsCommandOptions.ExecuteToolCommandOptions cliOptions = this.toolCommandOptions.executeToolCommandOptions;
        toolRunner.execute(cliOptions.toolId, new ObjectMap(cliOptions.params), Paths.get(cliOptions.outDir), sessionId);
    }

    private void executeJob() throws CatalogException, ToolException {
        ToolsCommandOptions.ExecuteJobCommandOptions cliOptions = this.toolCommandOptions.executeJobCommandOptions;
        toolRunner.execute(cliOptions.job, sessionId);
    }

    private void listTools() {
        Collection<Class<? extends OpenCgaTool>> tools = new ToolFactory().getTools();
        int toolIdSize = tools.stream().mapToInt(c -> c.getAnnotation(Tool.class).id().length()).max().orElse(0);
        System.out.println(StringUtils.rightPad("#Tool", toolIdSize) + "\tType\tDescription");
        for (Class<? extends OpenCgaTool> tool : tools) {
            Tool annotation = tool.getAnnotation(Tool.class);
            if (toolCommandOptions.listToolCommandOptions.type != null) {
                if (!annotation.type().equals(toolCommandOptions.listToolCommandOptions.type)) {
                    continue;
                }
            }
            System.out.println(StringUtils.rightPad(annotation.id(), toolIdSize) + "\t" + annotation.type() + "\t" + annotation.description());
        }
    }
}
