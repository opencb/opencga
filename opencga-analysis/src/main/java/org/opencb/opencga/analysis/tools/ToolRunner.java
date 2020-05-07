/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.tools;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ToolRunner {

    private Logger logger = LoggerFactory.getLogger(ToolRunner.class);

    private final CatalogManager catalogManager;
    private final StorageEngineFactory storageEngineFactory;
    private final String opencgaHome;
    private final ToolFactory toolFactory;

    public ToolRunner(String opencgaHome, CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        this.opencgaHome = opencgaHome;
        this.catalogManager = catalogManager;

        this.storageEngineFactory = storageEngineFactory;
        this.toolFactory = new ToolFactory();
    }

    /**
     * Execute a command tool.
     * @param jobId jobId of the job containing the relevant information.
     * @param token session id of the user that will execute the tool.
     * @return Execution result
     * @throws ToolException if the execution fails
     */
    public ExecutionResult execute(String jobId, String token) throws CatalogException, ToolException {
        // We get the job information.
        Query query = new Query();
        query.put(JobDBAdaptor.QueryParams.UID.key(), jobId);
        Job job = catalogManager.getJobManager().search(null, query, QueryOptions.empty(), token).first();

        return execute(job.getTool().getId(), new ObjectMap(job.getParams()), Paths.get(job.getOutDir().getUri()), token);
    }

    /**
     * Execute a tool
     * @param toolId Tool identifier. It can be either the tool id itself, or the class name.
     * @param params Params for the execution.
     * @param outDir Output directory. Mandatory
     * @param token session id of the user that will execute the tool.
     * @return Execution result
     * @throws ToolException if the execution fails
     */
    public ExecutionResult execute(String toolId, ObjectMap params, Path outDir, String token) throws ToolException {
        return toolFactory
                .createTool(toolId)
                .setUp(opencgaHome, catalogManager, storageEngineFactory, params, outDir, token)
                .start();
    }

    /**
     * Execute a tool
     * @param tool Tool class
     * @param params Params for the execution.
     * @param outDir Output directory. Mandatory
     * @param token session id of the user that will execute the tool.
     * @return Execution result
     * @throws ToolException if the execution fails
     */
    public ExecutionResult execute(Class<? extends OpenCgaTool> tool, ObjectMap params, Path outDir, String token) throws ToolException {
        return toolFactory
                .createTool(tool)
                .setUp(opencgaHome, catalogManager, storageEngineFactory, params, outDir, token)
                .start();
    }

    /**
     * Execute a command tool.
     * @param jobId jobId of the job containing the relevant information.
     * @param sessionId session id of the user that will execute the tool.
     */
    public void execute(long jobId, String sessionId) {
//        try {
//            // We get the job information.
//            Query query = new Query();
//            query.put(JobDBAdaptor.QueryParams.UID.key(), jobId);
//            Job job = jobManager.search(null, query, QueryOptions.empty(), sessionId).first();
//            long studyUid = jobManager.getStudyId(jobId);
//
//            // get the study FQN we need
//            query = new Query();
//            query.put(StudyDBAdaptor.QueryParams.UID.key(), studyUid);
//            String studyFqn = catalogManager.getStudyManager().get(query, QueryOptions.empty(), sessionId).first().getFqn();
//
//            String outDir = (String) job.getAttributes().get(Job.OPENCGA_TMP_DIR);
//            Path outDirPath = Paths.get(outDir);
//
//            String tool = job.getToolId();
//            String execution = job.getExecution();
//
//            // Create the OpenCGA output folder
//            fileManager.createFolder(studyFqn, (String) job.getAttributes().get(Job.OPENCGA_OUTPUT_DIR),
//                    new File.FileStatus(), true, "", QueryOptions.empty(), sessionId);
//
//            // Convert the input and output files to uris in the filesystem
//            Map<String, String> params = new HashMap<>(job.getParams());
//            List<Param> inputParams = toolManager.getInputParams(tool, execution);
//            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.URI.key());
//            for (Param inputParam : inputParams) {
//                if (inputParam.isRequired() && !params.containsKey(inputParam.getName())) {
//                    throw new CatalogException("Missing mandatory input parameter " + inputParam.getName());
//                }
//                if (params.containsKey(inputParam.getName())) {
//                    // Get the file uri
//                    String fileString = params.get(inputParam.getName());
//                    DataResult<File> fileQueryResult = fileManager.get(studyFqn, fileString, options, sessionId);
//                    if (fileQueryResult.getNumResults() == 0) {
//                        throw new CatalogException("File " + fileString + " not found");
//                    }
//                    params.put(inputParam.getName(), fileQueryResult.first().getUri().getPath());
//                }
//            }
//
//            // Convert output file params to be stored in the output directory specified
//            List<Param> outputParams = toolManager.getOutputParams(tool, execution);
//            for (Param outputParam : outputParams) {
//                if (outputParam.isRequired() && !params.containsKey(outputParam.getName())) {
//                    throw new CatalogException("Missing mandatory output parameter " + outputParam.getName());
//                }
//                if (params.containsKey(outputParam.getName())) {
//                    // Contextualise the file name in the uri where it should be written. /jobs/jobX/file.txt where /jobs/jobX = outDir and
//                    // file.txt = outputFileName
//                    Path name = Paths.get(params.get(outputParam.getName()));
//                    String absolutePath = outDirPath.resolve(name.toFile().getName()).toString();
//                    params.put(outputParam.getName(), absolutePath);
//                }
//            }
//
//            // Execute the tool
//            String commandLine = toolManager.createCommandLine(tool, execution, params);
//            toolManager.runCommandLine(commandLine, Paths.get(outDir), false);
//        } catch (CatalogException | AnalysisToolException e) {
//            logger.error("{}", e.getMessage(), e);
//        }
    }
}
