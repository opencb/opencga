package org.opencb.opencga.analysis;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;
import org.opencb.hpg.bigdata.analysis.tools.ToolManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.JobManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public class ToolAnalysis {

    private Logger logger = LoggerFactory.getLogger(ToolAnalysis.class);

    private CatalogManager catalogManager;
    private ToolManager toolManager;

    private JobManager jobManager;
    private FileManager fileManager;

    public ToolAnalysis(Configuration configuration) throws CatalogException, AnalysisToolException {
        this.catalogManager = new CatalogManager(configuration);
        this.toolManager = new ToolManager(Paths.get(configuration.getToolDir()));

        this.jobManager = catalogManager.getJobManager();
        this.fileManager = catalogManager.getFileManager();
    }

    /**
     * Execute a command tool.
     *  @param jobId jobId of the job containing the relevant information.
     * @param outDir directory path where the results will be stored.
     * @param sessionId session id of the user that will execute the tool.
     */
    public void execute(long jobId, String outDir, String sessionId) {
        try {
            // We get the job information.
            Job job = jobManager.get(jobId, QueryOptions.empty(), sessionId).first();
            long studyId = jobManager.getStudyId(jobId);

            String tool = job.getToolId();
            String execution = job.getExecution();

            // Create the OpenCGA output folder
            fileManager.createFolder(String.valueOf(studyId), (String) job.getAttributes().get(Job.OPENCGA_OUTPUT_DIR),
                    new File.FileStatus(), true, "", QueryOptions.empty(), sessionId);

            // Execute the tool
            String commandLine = toolManager.createCommandLine(tool, execution, job.getParams());
            toolManager.runCommandLine(commandLine, Paths.get(outDir));
        } catch (CatalogException | AnalysisToolException e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

}
