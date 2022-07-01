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

package org.opencb.opencga.master.monitor.daemons;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.alignment.qc.AlignmentQcAnalysis;
import org.opencb.opencga.analysis.variant.operations.VariantAnnotationIndexOperationTool;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.FileContent;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.job.*;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.StudyNotification;
import org.opencb.opencga.core.models.study.StudyUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;
import org.opencb.opencga.core.tools.result.JobResult;
import org.opencb.opencga.core.tools.result.Status;
import org.opencb.opencga.master.monitor.executors.BatchExecutor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class ExecutionDaemonTest extends AbstractManagerTest {

    private ExecutionDaemon executionDaemon;
    private JobDaemon jobDaemon;
    private DummyBatchExecutor executor;

    @Override
    @Before
    public void setUp() throws IOException, CatalogException {
        super.setUp();

        String expiringToken = this.catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
        String nonExpiringToken = this.catalogManager.getUserManager().getNonExpiringToken("opencga", expiringToken);
        catalogManager.getConfiguration().getAnalysis().getExecution().getMaxConcurrentJobs().put(VariantIndexOperationTool.ID, 1);

        jobDaemon = new JobDaemon(1000, nonExpiringToken, catalogManager, "/tmp");
        executionDaemon = new ExecutionDaemon(1000, nonExpiringToken, catalogManager);
        executor = new DummyBatchExecutor();
        jobDaemon.batchExecutor = executor;
    }

    @Test
    public void testBuildCli() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("key", "value");
        params.put("camelCaseKey", "value");
        params.put("flag", "");
        params.put("boolean", "true");
        params.put("outdir", "/tmp/folder");
        params.put("paramWithSpaces", "This could be a description");
        params.put("paramWithSingleQuotes", "This could 'be' a description");
        params.put("paramWithDoubleQuotes", "This could \"be\" a description");

        Map<String, String> dynamic1 = new LinkedHashMap<>();
        dynamic1.put("dynamic", "It's true");
        dynamic1.put("param with spaces", "Fuc*!");

        params.put("dynamicParam1", dynamic1);
        params.put("dynamicNested2", dynamic1);
        String cli = JobDaemon.buildCli("opencga-internal.sh", "variant index-run", params);
        assertEquals("opencga-internal.sh variant index-run "
                + "--key value "
                + "--camel-case-key value "
                + "--flag  "
                + "--boolean true "
                + "--outdir '/tmp/folder' "
                + "--param-with-spaces 'This could be a description' "
                + "--param-with-single-quotes 'This could '\"'\"'be'\"'\"' a description' "
                + "--param-with-double-quotes 'This could \"be\" a description' "
                + "--dynamic-param1 dynamic='It'\"'\"'s true' "
                + "--dynamic-param1 'param with spaces'='Fuc*!' "
                + "--dynamic-nested2 dynamic='It'\"'\"'s true' "
                + "--dynamic-nested2 'param with spaces'='Fuc*!'", cli);
    }

    @Test
    public void testCreateDefaultOutDir() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        String executionId = catalogManager.getExecutionManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, token).first().getId();

        executionDaemon.checkPendingExecutions();
        List<Job> jobs = getExecution(executionId).getJobs();
        assertEquals(1, jobs.size());
        String jobId = jobs.get(0).getId();
        jobDaemon.checkPendingJobs();

        URI uri = getJob(jobId).getOutDir().getUri();
        Assert.assertTrue(Files.exists(Paths.get(uri)));
        assertTrue(uri.getPath().startsWith(catalogManager.getConfiguration().getJobDir()) && uri.getPath().endsWith("files-delete/"));
    }

    @Test
    public void testWebhookNotification() throws Exception {
        catalogManager.getStudyManager().update(studyFqn, new StudyUpdateParams()
                .setNotification(new StudyNotification(new URL("https://ptsv2.com/t/dgogf-1581523512/post"))), null, token);

        HashMap<String, Object> params = new HashMap<>();
        String executionId = catalogManager.getExecutionManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, token).first().getId();

        executionDaemon.checkPendingExecutions();
        List<Job> jobs = getExecution(executionId).getJobs();
        assertEquals(1, jobs.size());
        String jobId = jobs.get(0).getId();
        jobDaemon.checkPendingJobs();
        // We sleep because there must be a thread sending notifying to the webhook url.
        Thread.sleep(1500);

        Job job = getJob(jobId);
        assertEquals(1, job.getInternal().getEvents().size());
        assertTrue(job.getInternal().getWebhook().getStatus().containsKey("QUEUED"));
        assertEquals(JobInternalWebhook.Status.ERROR, job.getInternal().getWebhook().getStatus().get("QUEUED"));
    }

//    @Test
//    public void testCreateOutDir() throws Exception {
//        HashMap<String, Object> params = new HashMap<>();
//        params.put("outdir", "outputDir/");
//        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, token).first().getId();
//
//        jobDaemon.checkPendingJobs();
//
//        URI uri = getJob(jobId).getOutDir().getUri();
//        Assert.assertTrue(Files.exists(Paths.get(uri)));
//        assertTrue(!uri.getPath().startsWith("/tmp/opencga/jobs/") && uri.getPath().endsWith("outputDir/"));
//    }
//
//    @Test
//    public void testUseEmptyDirectory() throws Exception {
//        // Create empty directory that is registered in OpenCGA
//        org.opencb.opencga.core.models.file.File directory = catalogManager.getFileManager().createFolder(studyFqn, "outputDir/",
//                true, "", QueryOptions.empty(), token).first();
//        catalogManager.getIoManagerFactory().get(directory.getUri()).createDirectory(directory.getUri(), true);
//
//        HashMap<String, Object> params = new HashMap<>();
//        params.put("outdir", "outputDir/");
//        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params,
//                token).first().getId();
//
//        jobDaemon.checkPendingJobs();
//
//        URI uri = getJob(jobId).getOutDir().getUri();
//        Assert.assertTrue(Files.exists(Paths.get(uri)));
//        assertTrue(!uri.getPath().startsWith("/tmp/opencga/jobs/") && uri.getPath().endsWith("outputDir/"));
//    }
//
//    @Test
//    public void testNotEmptyOutDir() throws Exception {
//        HashMap<String, Object> params = new HashMap<>();
//        params.put("outdir", "data/");
//        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, token).first().getId();
//
//        jobDaemon.checkPendingJobs();
//
//        OpenCGAResult<Job> jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId, QueryOptions.empty(), token);
//        assertEquals(1, jobOpenCGAResult.getNumResults());
//        checkStatus(getJob(jobId), Enums.ExecutionStatus.ABORTED);
//        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.ABORTED);
//        assertTrue(jobOpenCGAResult.first().getInternal().getStatus().getDescription().contains("not an empty directory"));
//    }

    @Test
    public void testProjectScopeTask() throws Exception {
        // User 2 to admins group in study1 but not in study2
        catalogManager.getStudyManager().updateGroup(studyFqn, "@admins", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("user2")), token);

        // User 3 to admins group in both study1 and study2
        catalogManager.getStudyManager().updateGroup(studyFqn, "@admins", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("user3")), token);
        catalogManager.getStudyManager().updateGroup(studyFqn2, "@admins", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("user3")), token);

        HashMap<String, Object> params = new HashMap<>();
        String executionId = catalogManager.getExecutionManager().submit(studyFqn, VariantAnnotationIndexOperationTool.ID, Enums.Priority.MEDIUM,
                params, token).first().getId();
        String executionId2 = catalogManager.getExecutionManager().submit(studyFqn, VariantAnnotationIndexOperationTool.ID, Enums.Priority.MEDIUM,
                params, sessionIdUser2).first().getId();
        String executionId3 = catalogManager.getExecutionManager().submit(studyFqn, VariantAnnotationIndexOperationTool.ID, Enums.Priority.MEDIUM,
                params, sessionIdUser3).first().getId();

        executionDaemon.checkPendingExecutions();
        String jobId = getExecution(executionId).getJobs().get(0).getId();
        String jobId2 = getExecution(executionId2).getJobs().get(0).getId();
        String jobId3 = getExecution(executionId3).getJobs().get(0).getId();
        jobDaemon.checkPendingJobs();

        // Job sent by the owner
        OpenCGAResult<Job> jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId, QueryOptions.empty(), token);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.QUEUED);

        // Job sent by user2 (admin from study1 but not from study2)
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId2, QueryOptions.empty(), token);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.ABORTED);
        assertTrue(jobOpenCGAResult.first().getInternal().getStatus().getDescription()
                .contains("can only be executed by the project owners or members of " + ParamConstants.ADMINS_GROUP));

        // Job sent by user3 (admin from study1 and study2)
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId3, QueryOptions.empty(), token);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.QUEUED);

        jobOpenCGAResult = catalogManager.getJobManager().search(studyFqn2, new Query(), QueryOptions.empty(), token);
        assertEquals(0, jobOpenCGAResult.getNumResults());

        jobOpenCGAResult = catalogManager.getJobManager().search(studyFqn2, new Query(),
                new QueryOptions(ParamConstants.OTHER_STUDIES_FLAG, true), token);
        assertEquals(2, jobOpenCGAResult.getNumResults());
    }

    @Test
    public void testDependsOnExecutions() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        String execution1 = catalogManager.getExecutionManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, token)
                .first().getId();
        String execution2 = catalogManager.getExecutionManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, null, null,
                Collections.singletonList(execution1), null, token).first().getId();

        executionDaemon.checkPendingExecutions();
        jobDaemon.checkPendingJobs();

        OpenCGAResult<Execution> executionResult = catalogManager.getExecutionManager().get(studyFqn, execution1, QueryOptions.empty(), token);
        assertEquals(1, executionResult.getNumResults());
        assertEquals(Enums.ExecutionStatus.PROCESSED, executionResult.first().getInternal().getStatus().getId());

        OpenCGAResult<Job> jobResult = catalogManager.getJobManager().get(studyFqn, executionResult.first().getJobs().get(0).getId(), QueryOptions.empty(), token);
        assertEquals(1, jobResult.getNumResults());
        assertEquals(Enums.ExecutionStatus.QUEUED, jobResult.first().getInternal().getStatus().getId());

        executionResult = catalogManager.getExecutionManager().get(studyFqn, execution2, QueryOptions.empty(), token);
        assertEquals(1, executionResult.getNumResults());
        assertEquals(Enums.ExecutionStatus.PENDING, executionResult.first().getInternal().getStatus().getId());

        // Set the status of job1 to ERROR
        catalogManager.getJobManager().privateUpdate(studyFqn, jobResult.first().getId(),
                new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR)), adminToken);

        executionDaemon.checkExecutions();
        executionResult = catalogManager.getExecutionManager().get(studyFqn, execution1, QueryOptions.empty(), token);
        assertEquals(1, executionResult.getNumResults());
        assertEquals(Enums.ExecutionStatus.ERROR, executionResult.first().getInternal().getStatus().getId());

        executionDaemon.checkExecutions();
        executionResult = catalogManager.getExecutionManager().get(studyFqn, execution2, QueryOptions.empty(), token);
        assertEquals(1, executionResult.getNumResults());
        assertEquals(Enums.ExecutionStatus.ABORTED, executionResult.first().getInternal().getStatus().getId());
        assertTrue(executionResult.first().getInternal().getStatus().getDescription().contains("depended on"));
    }

    @Test
    public void testDependsOnMultiStudy() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        Execution firstJob = catalogManager.getExecutionManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, token).first();
        Execution job = catalogManager.getExecutionManager().submit(studyFqn2, "files-delete", Enums.Priority.MEDIUM, params, null, null,
                Collections.singletonList(firstJob.getUuid()), null, token).first();
        assertEquals(1, job.getDependsOn().size());
        assertEquals(firstJob.getId(), job.getDependsOn().get(0).getId());
        assertEquals(firstJob.getUuid(), job.getDependsOn().get(0).getUuid());

        job = catalogManager.getExecutionManager().get(studyFqn2, job.getId(), QueryOptions.empty(), token).first();
        assertEquals(1, job.getDependsOn().size());
        assertEquals(firstJob.getId(), job.getDependsOn().get(0).getId());
        assertEquals(firstJob.getUuid(), job.getDependsOn().get(0).getUuid());
    }

    @Test
    public void testRunJob() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(JobDaemon.OUTDIR_PARAM, "outDir");
        org.opencb.opencga.core.models.file.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, token).first();
        params.put("myFile", inputFile.getPath());
        Execution execution = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, token).first();
        executionDaemon.checkPendingExecutions();
        String jobId = getExecution(execution.getId()).getJobs().get(0).getId();

        jobDaemon.checkJobs();

        String[] cli = getJob(jobId).getCommandLine().split(" ");
        int i = Arrays.asList(cli).indexOf("--my-file");
        assertEquals("'" + inputFile.getPath() + "'", cli[i + 1]);
        assertEquals(1, getJob(jobId).getInput().size());
        assertEquals(inputFile.getPath(), getJob(jobId).getInput().get(0).getPath());
        checkStatus(getJob(jobId), Enums.ExecutionStatus.QUEUED);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        jobDaemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.RUNNING);
        createAnalysisResult(jobId, "myTest", ar -> ar.setStatus(new Status(Status.Type.DONE, null, TimeUtils.getDate())));
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.READY);

        jobDaemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.DONE);
    }

    @Test
    public void testCheckLogs() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        org.opencb.opencga.core.models.file.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, token).first();
        params.put("myFile", inputFile.getPath());
        Execution exec = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, token).first();
        String executionId = exec.getId();

        executionDaemon.checkPendingExecutions();
        List<Job> jobs = getExecution(executionId).getJobs();
        assertEquals(1, jobs.size());
        String jobId = jobs.get(0).getId();
        jobDaemon.checkJobs();

        String[] cli = getJob(jobId).getCommandLine().split(" ");
        int i = Arrays.asList(cli).indexOf("--my-file");
        assertEquals("'" + inputFile.getPath() + "'", cli[i + 1]);
        assertEquals(1, getJob(jobId).getInput().size());
        assertEquals(inputFile.getPath(), getJob(jobId).getInput().get(0).getPath());
        checkStatus(getJob(jobId), Enums.ExecutionStatus.QUEUED);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        Job job = catalogManager.getJobManager().get(studyFqn, jobId, null, token).first();

        jobDaemon.checkJobs();
        assertEquals(Enums.ExecutionStatus.RUNNING, getJob(jobId).getInternal().getStatus().getId());

        InputStream inputStream = new ByteArrayInputStream("my log content\nlast line".getBytes(StandardCharsets.UTF_8));
        catalogManager.getIoManagerFactory().getDefault().copy(inputStream,
                Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".log").toUri());

        OpenCGAResult<FileContent> fileContentResult = catalogManager.getJobManager().log(studyFqn, jobId, 0, 1, "stdout", true, token);
        assertEquals("last line", fileContentResult.first().getContent());

        fileContentResult = catalogManager.getJobManager().log(studyFqn, jobId, 0, 1, "stdout", false, token);
        assertEquals("my log content\n", fileContentResult.first().getContent());
    }

//    @Test
//    public void testRegisterFilesSuccessfully() throws Exception {
//        HashMap<String, Object> params = new HashMap<>();
////        params.put(JobDaemon.OUTDIR_PARAM, "outDir");
//        org.opencb.opencga.core.models.file.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, token).first();
//        params.put("myFile", inputFile.getPath());
//        Execution job = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, token).first();
//        String jobId = job.getId();
//
//        jobDaemon.checkJobs();
//
//        String[] cli = getJob(jobId).getCommandLine().split(" ");
//        int i = Arrays.asList(cli).indexOf("--my-file");
//        assertEquals("'" + inputFile.getPath() + "'", cli[i + 1]);
//        assertEquals(1, getJob(jobId).getInput().size());
//        assertEquals(inputFile.getPath(), getJob(jobId).getInput().get(0).getPath());
//        assertEquals(Enums.ExecutionStatus.QUEUED, getJob(jobId).getInternal().getStatus().getId());
//        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);
//
//        jobDaemon.checkJobs();
//
//        assertEquals(Enums.ExecutionStatus.RUNNING, getJob(jobId).getInternal().getStatus().getId());
//        createAnalysisResult(jobId, "myTest", ar -> ar.setStatus(new Status(Status.Type.DONE, null, TimeUtils.getDate())));
//        executor.jobStatus.put(jobId, Enums.ExecutionStatus.READY);
//
//        job = catalogManager.getJobManager().get(studyFqn, job.getId(), QueryOptions.empty(), token).first();
//        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("file1.txt"));
//        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("file2.txt"));
//        Files.createDirectory(Paths.get(job.getOutDir().getUri()).resolve("A"));
//        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("A/file3.txt"));
//
//        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".log"));
//        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".err"));
//
//        jobDaemon.checkJobs();
//
//        assertEquals(Enums.ExecutionStatus.DONE, getJob(jobId).getInternal().getStatus().getId());
//
//        job = catalogManager.getJobManager().get(studyFqn, job.getId(), QueryOptions.empty(), token).first();
//
//        String outDir = job.getOutDir().getPath();
//
//        assertEquals(4, job.getOutput().size());
//        for (org.opencb.opencga.core.models.file.File file : job.getOutput()) {
//            assertTrue(Arrays.asList(outDir + "file1.txt", outDir + "file2.txt", outDir + "A/", outDir + "A/file3.txt")
//                    .contains(file.getPath()));
//        }
//        assertEquals(0, job.getOutput().stream().filter(f -> f.getName().endsWith(ExecutionResultManager.FILE_EXTENSION))
//                .collect(Collectors.toList()).size());
//
//        assertEquals(job.getId() + ".log", job.getStdout().getName());
//        assertEquals(job.getId() + ".err", job.getStderr().getName());
//
//        // Check jobId is properly populated
//        OpenCGAResult<org.opencb.opencga.core.models.file.File> files = catalogManager.getFileManager().search(studyFqn,
//                new Query(FileDBAdaptor.QueryParams.JOB_ID.key(), job.getId()), FileManager.INCLUDE_FILE_URI_PATH, token);
//        assertEquals(7, files.getNumResults());
//        for (org.opencb.opencga.core.models.file.File file : files.getResults()) {
//            assertTrue(Arrays.asList(outDir, outDir + "file1.txt", outDir + "file2.txt", outDir + "A/", outDir + "A/file3.txt",
//                    outDir + "" + job.getId() + ".log", outDir + "" + job.getId() + ".err").contains(file.getPath()));
//        }
//
//        files = catalogManager.getFileManager().count(studyFqn, new Query(FileDBAdaptor.QueryParams.JOB_ID.key(), ""), token);
//        assertEquals(10, files.getNumMatches());
//        files = catalogManager.getFileManager().count(studyFqn, new Query(FileDBAdaptor.QueryParams.JOB_ID.key(), "NonE"), token);
//        assertEquals(10, files.getNumMatches());
//    }
//
//    @Test
//    public void testRunJobFail() throws Exception {
//        HashMap<String, Object> params = new HashMap<>();
//        params.put(JobDaemon.OUTDIR_PARAM, "outputDir/");
//        Job job = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, token).first();
//        String jobId = job.getId();
//
//        jobDaemon.checkJobs();
//
//        assertEquals(Enums.ExecutionStatus.QUEUED, getJob(jobId).getInternal().getStatus().getId());
//        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);
//
//        jobDaemon.checkJobs();
//
//        assertEquals(Enums.ExecutionStatus.RUNNING, getJob(jobId).getInternal().getStatus().getId());
//        createAnalysisResult(jobId, "myTest", ar -> ar.setStatus(new Status(Status.Type.ERROR, null, TimeUtils.getDate())));
//        executor.jobStatus.put(jobId, Enums.ExecutionStatus.ERROR);
//
//        jobDaemon.checkJobs();
//
//        assertEquals(Enums.ExecutionStatus.ERROR, getJob(jobId).getInternal().getStatus().getId());
//        assertEquals("Job could not finish successfully", getJob(jobId).getInternal().getStatus().getDescription());
//    }

    @Test
    public void testRunJobFailMissingExecutionResult() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        Execution execution = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, token)
                .first();

        executionDaemon.checkPendingExecutions();
        List<Job> jobs = getExecution(execution.getId()).getJobs();
        assertEquals(1, jobs.size());
        String jobId = jobs.get(0).getId();
        assertEquals(Enums.ExecutionStatus.PENDING, jobs.get(0).getInternal().getStatus().getId());

        jobDaemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.QUEUED, getJob(jobId).getInternal().getStatus().getId());
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        jobDaemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.RUNNING, getJob(jobId).getInternal().getStatus().getId());
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.READY);

        jobDaemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.ERROR, getJob(jobId).getInternal().getStatus().getId());
        assertEquals("Job could not finish successfully. Missing execution result", getJob(jobId).getInternal().getStatus().getDescription());
    }

    @Test
    public void fastQCAnalysisPipelineTest() throws IOException, CatalogException, URISyntaxException {
        String bamUri = getClass().getResource("/biofiles/HG00096.chrom20.small.bam").toURI().toString();
        catalogManager.getFileManager().link(studyFqn, new FileLinkParams().setUri(bamUri).setPath("."), false, token);

        // Load and create the pipeline
        PipelineCreateParams pipeline = PipelineCreateParams.load(getClass().getResource("/pipelines-test/alignment-qc-2.yml").openStream());
        catalogManager.getPipelineManager().create(studyFqn, pipeline, QueryOptions.empty(), token);

        // Submit an alignment-qc execution
        Map<String, Object> params = new HashMap<>();
        params.put("bamFile", "HG00096.chrom20.small.bam");
        params.put("bedFile", "HG00096.chrom20.small.bam");
        params.put("dictFile", "HG00096.chrom20.small.bam");
        OpenCGAResult<Execution> executionResult = catalogManager.getExecutionManager().submit(studyFqn,
                AlignmentQcAnalysis.ID, Enums.Priority.MEDIUM, params, "", "", null, null, token);
        assertNotNull(executionResult.first().getOutDir());

        executionDaemon.checkPendingExecutions();
        jobDaemon.checkBlockedJobs();
        jobDaemon.checkPendingJobs();

        executionResult = catalogManager.getExecutionManager().get(studyFqn, executionResult.first().getId(), QueryOptions.empty(), token);
        assertEquals(1, executionResult.getNumResults());
        assertEquals(Enums.ExecutionStatus.PROCESSED, executionResult.first().getInternal().getStatus().getId());
        assertEquals(4, executionResult.first().getJobs().size());
        for (int i = 0; i < executionResult.first().getJobs().size(); i++) {
            Job job = executionResult.first().getJobs().get(i);
            if (i == 1 | i == 2) {
                assertEquals(Enums.ExecutionStatus.PENDING, job.getInternal().getStatus().getId());
            } else if (i == 0) {
                assertEquals(Enums.ExecutionStatus.BLOCKED, job.getInternal().getStatus().getId());
                assertTrue(job.getInternal().getStatus().getDescription().contains("checked shortly"));
            } else {
                assertEquals(Enums.ExecutionStatus.BLOCKED, job.getInternal().getStatus().getId());
                assertTrue(job.getInternal().getStatus().getDescription().contains("dependencies to finish"));
            }
        }

        jobDaemon.checkBlockedJobs();
        executionResult = catalogManager.getExecutionManager().get(studyFqn, executionResult.first().getId(), QueryOptions.empty(), token);
        assertEquals(1, executionResult.getNumResults());
        assertEquals(Enums.ExecutionStatus.PROCESSED, executionResult.first().getInternal().getStatus().getId());
        assertEquals(4, executionResult.first().getJobs().size());
        for (int i = 0; i < executionResult.first().getJobs().size(); i++) {
            Job job = executionResult.first().getJobs().get(i);
            if (i < 3) {
                assertEquals(Enums.ExecutionStatus.PENDING, job.getInternal().getStatus().getId());
            } else {
                assertEquals(Enums.ExecutionStatus.BLOCKED, job.getInternal().getStatus().getId());
                assertTrue(job.getInternal().getStatus().getDescription().contains("dependencies to finish"));
            }
        }

        // Update job status to DONE
        Job job = executionResult.first().getJobs().get(0);
        catalogManager.getJobManager().privateUpdate(studyFqn, job.getId(),
                new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE)), adminToken);
        jobDaemon.checkBlockedJobs();
        executionResult = catalogManager.getExecutionManager().get(studyFqn, executionResult.first().getId(), QueryOptions.empty(), token);
        assertEquals(1, executionResult.getNumResults());
        assertEquals(Enums.ExecutionStatus.PROCESSED, executionResult.first().getInternal().getStatus().getId());
        assertEquals(4, executionResult.first().getJobs().size());
        for (int i = 0; i < executionResult.first().getJobs().size(); i++) {
            Job tmpjob = executionResult.first().getJobs().get(i);
            if (i == 0) {
                assertEquals(Enums.ExecutionStatus.DONE, tmpjob.getInternal().getStatus().getId());
            } else {
                assertEquals(Enums.ExecutionStatus.PENDING, tmpjob.getInternal().getStatus().getId());
            }
        }

//        executionDaemon.checkPendingExecutions();
//        executionResult = catalogManager.getExecutionManager().get(studyFqn, executionResult.first().getId(), QueryOptions.empty(), token);
//        assertEquals(1, executionResult.getNumResults());
//        assertEquals(Enums.ExecutionStatus.PROCESSED, executionResult.first().getInternal().getStatus().getId());
//        assertEquals(4, executionResult.first().getJobs().size());
//        Job stats = executionResult.first().getJobs().get(0);
//        job = executionResult.first().getJobs().get(3);
//        assertEquals("alignment-stats uuid: " + stats.getUuid(), job.getDescription());
    }

    @Test
    public void executionToolTest() throws CatalogException, URISyntaxException {
        String bamUri = getClass().getResource("/biofiles/HG00096.chrom20.small.bam").toURI().toString();
        catalogManager.getFileManager().link(studyFqn, new FileLinkParams().setUri(bamUri).setPath("."), false, token);

        // Submit an alignment-qc execution
        Map<String, Object> params = new HashMap<>();
        params.put("bamFile", "HG00096.chrom20.small.bam");
        params.put("bedFile", "HG00096.chrom20.small.bam");
        params.put("dictFile", "HG00096.chrom20.small.bam");
        Execution execution = catalogManager.getExecutionManager().submit(studyFqn,
                AlignmentQcAnalysis.ID, Enums.Priority.MEDIUM, params, "", "", null, null, token).first();

        assertNotNull(execution);
        assertEquals(Enums.ExecutionStatus.PENDING, execution.getInternal().getStatus().getId());

        executionDaemon.checkPendingExecutions();
        execution = getExecution(execution.getId());
        assertEquals(1, execution.getJobs().size());

        assertEquals(1, execution.getJobs().size());
        for (Job job : execution.getJobs()) {
            assertEquals(Enums.ExecutionStatus.PENDING, job.getInternal().getStatus().getId());
        }
    }

    private void checkStatus(Job job, String status) {
        assertEquals(status, job.getInternal().getStatus().getId());
        assertEquals(status, job.getInternal().getStatus().getId());
    }

    private Job getJob(String jobId) throws CatalogException {
        return catalogManager.getJobManager().get(studyFqn, jobId, new QueryOptions(), token).first();
    }

    private Execution getExecution(String executionId) throws CatalogException {
        return catalogManager.getExecutionManager().get(studyFqn, executionId, new QueryOptions(), token).first();
    }

    private void createAnalysisResult(String jobId, String analysisId, Consumer<JobResult> c) throws CatalogException, IOException {
        JobResult ar = new JobResult();
        c.accept(ar);
        File resultFile = Paths.get(getJob(jobId).getOutDir().getUri()).resolve(analysisId + ExecutionResultManager.FILE_EXTENSION).toFile();
        JacksonUtils.getDefaultObjectMapper().writeValue(resultFile, ar);
    }

    private static class DummyBatchExecutor implements BatchExecutor {

        public Map<String, String> jobStatus = new HashMap<>();

        @Override
        public void execute(String jobId, String queue, String commandLine, Path stdout, Path stderr) throws Exception {
            System.out.println("Executing job " + jobId + " --- " + commandLine);
            jobStatus.put(jobId, Enums.ExecutionStatus.QUEUED);
        }

        @Override
        public String getStatus(String jobId) {
            return jobStatus.getOrDefault(jobId, Enums.ExecutionStatus.UNKNOWN);
        }

        @Override
        public boolean isExecutorAlive() {
            return true;
        }

        @Override
        public boolean stop(String jobId) throws Exception {
            return false;
        }

        @Override
        public boolean resume(String jobId) throws Exception {
            return false;
        }

        @Override
        public boolean kill(String jobId) throws Exception {
            return false;
        }
    }
}
