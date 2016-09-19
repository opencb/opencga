package org.opencb.opencga.analysis;

import org.apache.tools.ant.types.Commandline;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.models.tool.Execution;
import org.opencb.opencga.catalog.monitor.exceptions.ExecutionException;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 30/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class JobFactory {

    private final CatalogManager catalogManager;
    protected static Logger logger = LoggerFactory.getLogger(JobFactory.class);

    public JobFactory(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public QueryResult<Job> createJob(ToolManager toolManager, Map<String, List<String>> params, long studyId, String jobName, String description,
                                      File outDir, List<Long> inputFiles, String sessionId)
            throws AnalysisExecutionException, CatalogException {
        return createJob(toolManager, params, studyId, jobName, description, outDir, inputFiles, sessionId, false);
    }

    public QueryResult<Job> createAndExecuteJob(ToolManager toolManager, Map<String, List<String>> params, long studyId, String jobName, String description,
                                      File outDir, List<Long> inputFiles, String sessionId)
            throws AnalysisExecutionException, CatalogException {
        return createJob(toolManager, params, studyId, jobName, description, outDir, inputFiles, sessionId, true);
    }

    /**
     * Create a catalog Job given a {@link ToolManager} and a set of params.
     *
     * Requires a ToolManager to create the command line.
     *
     * @param toolManager   {@link ToolManager} of the tool
     * @param params        Params to use with the tool
     * @param studyId       StudyId where to create the job
     * @param jobName       Job name
     * @param description   Job description
     * @param outDir        Output directory
     * @param inputFiles    Input files
     * @param sessionId     User sessionId
     * @param execute       Execute job locally before create
     * @return              New catalog job
     * @throws AnalysisExecutionException
     * @throws CatalogException
     */
    public QueryResult<Job> createJob(ToolManager toolManager, Map<String, List<String>> params, long studyId, String jobName, String description,
                                      File outDir, List<Long> inputFiles, String sessionId, boolean execute)
            throws AnalysisExecutionException, CatalogException {
        Execution execution = toolManager.getExecution();
        String executable = execution.getExecutable();

        // Create temporal Outdir
        String randomString = "J_" + StringUtils.randomString(10);
        URI temporalOutDirUri = catalogManager.createJobOutDir(studyId, randomString, sessionId);
        params.put(execution.getOutputParam(), Arrays.asList(temporalOutDirUri.getPath()));

        // Create commandLine
        String commandLine = toolManager.createCommandLine(executable, params);
        logger.debug("Command line : {}", commandLine);

        Map<String, String> plainParams = params.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().stream().collect(Collectors.joining(",")))
        );

        HashMap<String, Object> attributes = new HashMap<>();
        attributes.put("plugin", toolManager.isPlugin()); //TODO: Save type of tool in a better way

        return createJob(studyId, jobName, toolManager.getAnalysisName(), execution.getId(), plainParams, commandLine, description, outDir,
                temporalOutDirUri, inputFiles, randomString, attributes, new HashMap<>(), sessionId,
                false, execute);
    }

    @Deprecated
    public QueryResult<Job> createJob(long studyId, String jobName, String toolName, String description,
                                      File outDir, List<Long> inputFiles, final String sessionId,
                                      String randomString, URI temporalOutDirUri, String commandLine,
                                      boolean execute, boolean simulate, Map<String, Object> attributes,
                                      Map<String, Object> resourceManagerAttributes)
            throws AnalysisExecutionException, CatalogException {
        Map<String, String> params = getParamsFromCommandLine(commandLine);
        return createJob(studyId, jobName, toolName, "", params, commandLine, description, outDir, temporalOutDirUri, inputFiles,
                randomString, attributes, resourceManagerAttributes, sessionId, simulate, execute);
    }

    /**
     * Create a catalog Job given a commandLine and the rest of parameters.
     *
     * @param studyId                   Study id
     * @param jobName                   Job name
     * @param toolName                  Tool name
     * @param executor                  Tool executor name
     * @param params                    Map of params
     * @param commandLine               Command line to execute
     * @param description               Job description (optional)
     * @param outDir                    Final output directory
     * @param temporalOutDirUri         Temporal output directory
     * @param inputFiles                List of input files
     * @param jobSchedulerName          Name of the job in the job scheduler. Usually a random string.
     * @param attributes                Optional attributes
     * @param resourceManagerAttributes Optional resource manager attributes
     * @param sessionId                 User sessionId
     * @param simulate                  Simulate job creation. Do not create any job in catalog.
     * @param execute                   Execute job locally before create
     * @return              New catalog job
     * @throws AnalysisExecutionException
     * @throws CatalogException
     */
    public QueryResult<Job> createJob(long studyId, String jobName, String toolName, String executor, Map<String, String> params, String commandLine, String description,
                                      File outDir, URI temporalOutDirUri, List<Long> inputFiles, String jobSchedulerName, Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes, final String sessionId,
                                      boolean simulate, boolean execute)
            throws AnalysisExecutionException, CatalogException {
        logger.debug("Creating job {}: simulate {}, execute {}", jobName, simulate, execute);
        long start = System.currentTimeMillis();

        QueryResult<Job> jobQueryResult;
        if (resourceManagerAttributes == null) {
            resourceManagerAttributes = new HashMap<>();
        }

        resourceManagerAttributes.put(Job.JOB_SCHEDULER_NAME, jobSchedulerName);
        if (simulate) { //Simulate a job. Do not create it.
            jobQueryResult = new QueryResult<>("simulatedJob", (int) (System.currentTimeMillis() - start), 1, 1, "", "", Collections.singletonList(
                    new Job(-10, jobName, catalogManager.getUserIdBySessionId(sessionId), toolName,
                            TimeUtils.getTime(), description, start, System.currentTimeMillis(), "", commandLine, -1,
                            new Job.JobStatus(Job.JobStatus.PREPARED), -1, outDir.getId(), inputFiles, Collections.emptyList(),
                            null, attributes, resourceManagerAttributes)));
        } else {
            if (execute) {
                /** Create a RUNNING job in CatalogManager **/
                jobQueryResult = catalogManager.createJob(studyId, jobName, toolName, description, executor, params, commandLine, temporalOutDirUri,
                        outDir.getId(), inputFiles, null, attributes, resourceManagerAttributes, new Job.JobStatus(Job.JobStatus.RUNNING),
                        System.currentTimeMillis(), 0, null, sessionId);
                Job job = jobQueryResult.first();

                //Execute job in local
//                LocalExecutorManager executorManager = new LocalExecutorManager(catalogManager, sessionId);
//                jobQueryResult = executorManager.run(job);
                try {
                    ExecutorManager.execute(catalogManager, job, sessionId, "LOCAL");
                } catch (ExecutionException e) {
                    throw new AnalysisExecutionException(e.getCause());
                } catch (IOException e) {
                    throw new AnalysisExecutionException(e.getCause());
                }
                jobQueryResult = catalogManager.getJob(job.getId(), null, sessionId);

            } else {
                /** Create a PREPARED job in CatalogManager **/
                jobQueryResult = catalogManager.createJob(studyId, jobName, toolName, description, executor, params, commandLine, temporalOutDirUri,
                        outDir.getId(), inputFiles, null, attributes, resourceManagerAttributes, new Job.JobStatus(Job.JobStatus.PREPARED), 0, 0, null, sessionId);
            }
        }
        return jobQueryResult;
    }

    public static Map<String, String> getParamsFromCommandLine(String commandLine) {
        String[] args = Commandline.translateCommandline(commandLine);
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                String key = args[i].replaceAll("^--?", "");
                String value;
                if (args.length == i + 1 || args[i + 1].startsWith("-")) {
                    value = "";
                } else {
                    value = args[i + 1];
                    i++;
                }
                params.put(key, value);
            }
        }
        return params;
    }

}
