/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.old;

import org.apache.tools.ant.types.Commandline;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.monitor.exceptions.ExecutionException;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Deprecated
    public DataResult<Job> createJob(long studyId, String jobName, String toolName, String description,
                                      File outDir, List<File> inputFiles, final String sessionId,
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
    public DataResult<Job> createJob(long studyId, String jobName, String toolName, String executor, Map<String, String> params, String commandLine, String description,
                                      File outDir, URI temporalOutDirUri, List<File> inputFiles, String jobSchedulerName, Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes, final String sessionId,
                                      boolean simulate, boolean execute)
            throws AnalysisExecutionException, CatalogException {
        logger.debug("Creating job {}: simulate {}, execute {}", jobName, simulate, execute);
        long start = System.currentTimeMillis();

        DataResult<Job> jobQueryResult;
        if (resourceManagerAttributes == null) {
            resourceManagerAttributes = new HashMap<>();
        }

        resourceManagerAttributes.put(Job.JOB_SCHEDULER_NAME, jobSchedulerName);
        if (simulate) { //Simulate a job. Do not create it.
            jobQueryResult = new DataResult<>((int) (System.currentTimeMillis() - start), Collections.emptyList(), 1,
                    Collections.singletonList(new Job(jobName, catalogManager.getUserManager().getUserId(sessionId), toolName, description,
                            commandLine, outDir, inputFiles, 1)), 1);
        } else {
            if (execute) {
                /** Create a RUNNING job in CatalogManager **/
//                jobQueryResult = catalogManager.getJobManager().create(studyId, jobName, toolName, description, executor, params,
//                        commandLine, temporalOutDirUri, outDir.getUid(), inputFiles, null, attributes, resourceManagerAttributes, new Job
//                                .JobStatus(Job.JobStatus.RUNNING), System.currentTimeMillis(), (long) 0, null, sessionId);
//                Job job = jobQueryResult.first();
                Job job = null;

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
                jobQueryResult = catalogManager.getJobManager().get(job.getUid(), null, sessionId);

            } else {
                /** Create a PREPARED job in CatalogManager **/
//                jobQueryResult = catalogManager.getJobManager().create(studyId, jobName, toolName, description, executor, params,
//                        commandLine, temporalOutDirUri, outDir.getUid(), inputFiles, null, attributes, resourceManagerAttributes, new Job
//                                .JobStatus(Job.JobStatus.PREPARED), (long) 0, (long) 0, null, sessionId);
                jobQueryResult = null;
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
