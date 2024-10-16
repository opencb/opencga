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
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.operations.VariantAnnotationIndexOperationTool;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileContent;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobInternal;
import org.opencb.opencga.core.models.job.JobInternalWebhook;
import org.opencb.opencga.core.models.job.JobPermissions;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.StudyNotification;
import org.opencb.opencga.core.models.study.StudyUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;
import org.opencb.opencga.master.monitor.executors.BatchExecutor;
import org.opencb.opencga.master.monitor.models.PrivateJobUpdateParams;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class ExecutionDaemonTest extends AbstractManagerTest {

    private ExecutionDaemon daemon;
    private DummyBatchExecutor executor;

    private List<String> organizationIds;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        String expiringToken = this.catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
        catalogManager.getConfiguration().getAnalysis().getExecution().getMaxConcurrentJobs().put(VariantIndexOperationTool.ID, 1);

        daemon = new ExecutionDaemon(1000, expiringToken, catalogManager,
                new StorageConfiguration().setMode(StorageConfiguration.Mode.READ_WRITE), catalogManagerResource.getOpencgaHome().toString());

        executor = new DummyBatchExecutor();
        daemon.batchExecutor = executor;

        this.organizationIds = Arrays.asList(organizationId, ParamConstants.ADMIN_ORGANIZATION);
    }

    @Test
    public void testBuildCli() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("key", "value");
        params.put("camelCaseKey", "value");
        params.put("flag", "");
        params.put("boolean", true);
        params.put("outdir", "/tmp/folder");
        params.put("paramWithSpaces", "This could be a description");
        params.put("paramWithSingleQuotes", "This could 'be' a description");
        params.put("paramWithDoubleQuotes", "This could \"be\" a description");
        params.put("nullShouldBeIgnored", null);

        Map<String, Object> dynamic1 = new LinkedHashMap<>();
        dynamic1.put("dynamic", "It's true");
        dynamic1.put("param with spaces", "Fuc*!");
        dynamic1.put("boolean", false);
        dynamic1.put("number", 2354.23);
        dynamic1.put("nullShouldBeIgnored", null);

        params.put("dynamicParam1", dynamic1);
        params.put("dynamicNested2", dynamic1);
        String cli = ExecutionDaemon.buildCli("opencga-internal.sh", "variant index-run", params);
//        System.out.println("cli = " + cli);
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
                + "--dynamic-param1 boolean=false "
                + "--dynamic-param1 number='2354.23' "
                + "--dynamic-nested2 dynamic='It'\"'\"'s true' "
                + "--dynamic-nested2 'param with spaces'='Fuc*!' "
                + "--dynamic-nested2 boolean=false "
                + "--dynamic-nested2 number='2354.23'", cli);
    }

    @Test
    public void testCreateDefaultOutDir() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, ownerToken).first().getId();

        daemon.checkPendingJobs(organizationIds);

        URI uri = getJob(jobId).getOutDir().getUri();
        Assert.assertTrue(Files.exists(Paths.get(uri)));
        assertTrue(uri.getPath().startsWith(catalogManager.getConfiguration().getJobDir()) && uri.getPath().endsWith(jobId + "/"));
    }

    @Test
    public void testWebhookNotification() throws Exception {
        catalogManager.getStudyManager().update(studyFqn, new StudyUpdateParams()
                .setNotification(new StudyNotification(new URL("https://ptsv2.com/t/dgogf-1581523512/post"))), null, ownerToken);

        HashMap<String, Object> params = new HashMap<>();
        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, ownerToken).first().getId();

        daemon.checkPendingJobs(organizationIds);
        // We sleep because there must be a thread sending notifying to the webhook url.
        Thread.sleep(1500);

        Job job = getJob(jobId);
        assertEquals(1, job.getInternal().getEvents().size());
        assertTrue(job.getInternal().getWebhook().getStatus().containsKey("QUEUED"));
        assertEquals(JobInternalWebhook.Status.ERROR, job.getInternal().getWebhook().getStatus().get("QUEUED"));
    }

    @Test
    public void testCreateOutDir() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put("outdir", "outputDir/");
        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, ownerToken).first().getId();

        daemon.checkPendingJobs(organizationIds);

        URI uri = getJob(jobId).getOutDir().getUri();
        Assert.assertTrue(Files.exists(Paths.get(uri)));
        assertTrue(!uri.getPath().startsWith("/tmp/opencga/jobs/") && uri.getPath().endsWith("outputDir/"));
    }

    @Test
    public void testUseEmptyDirectory() throws Exception {
        // Create empty directory that is registered in OpenCGA
        org.opencb.opencga.core.models.file.File directory = catalogManager.getFileManager().createFolder(studyFqn, "outputDir/",
                true, "", QueryOptions.empty(), ownerToken).first();
        catalogManager.getIoManagerFactory().get(directory.getUri()).createDirectory(directory.getUri(), true);

        HashMap<String, Object> params = new HashMap<>();
        params.put("outdir", "outputDir/");
        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params,
                ownerToken).first().getId();

        daemon.checkPendingJobs(organizationIds);

        URI uri = getJob(jobId).getOutDir().getUri();
        Assert.assertTrue(Files.exists(Paths.get(uri)));
        assertTrue(!uri.getPath().startsWith("/tmp/opencga/jobs/") && uri.getPath().endsWith("outputDir/"));
    }

    @Test
    public void testNotEmptyOutDir() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put("outdir", "data/");
        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, ownerToken).first().getId();

        daemon.checkPendingJobs(organizationIds);

        OpenCGAResult<Job> jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(getJob(jobId), Enums.ExecutionStatus.ABORTED);
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.ABORTED);
        assertTrue(jobOpenCGAResult.first().getInternal().getStatus().getDescription().contains("not an empty directory"));
    }

    @Test
    public void dryRunExecutionTest() throws Exception {
        ObjectMap params = new ObjectMap();
        String jobId1 = catalogManager.getJobManager().submit(studyFqn, VariantAnnotationIndexOperationTool.ID, Enums.Priority.MEDIUM,
                params, "job1", "", null, null, null, null, true, ownerToken).first().getId();
        daemon.checkJobs();
        Job job = catalogManager.getJobManager().get(studyFqn, jobId1, null, ownerToken).first();

        StorageConfiguration storageConfiguration = new StorageConfiguration();
        storageConfiguration.getVariant().setDefaultEngine("mongodb");
        ToolRunner toolRunner = new ToolRunner(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration));
        toolRunner.execute(job, ownerToken);
        daemon.checkJobs();

        job = catalogManager.getJobManager().get(studyFqn, jobId1, null, ownerToken).first();
        assertEquals(Enums.ExecutionStatus.DONE, job.getInternal().getStatus().getId());
        assertEquals(1, job.getExecution().getSteps().size());
        assertEquals("check", job.getExecution().getSteps().get(0).getId());
    }

    @Test
    public void testProjectScopeTaskAndScheduler() throws Exception {
        // User 2 to admins group in study1 but not in study2
        catalogManager.getStudyManager().updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(normalUserId2)), ownerToken);

        // User 3 to admins group in both study1 and study2
        catalogManager.getStudyManager().updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(normalUserId3)), ownerToken);
        catalogManager.getStudyManager().updateGroup(studyFqn2, ParamConstants.ADMINS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(normalUserId3)), ownerToken);

        HashMap<String, Object> params = new HashMap<>();
        String jobId1 = catalogManager.getJobManager().submit(studyFqn, VariantAnnotationIndexOperationTool.ID, Enums.Priority.MEDIUM,
                params, "job1", "", null, null, null, null, false, normalToken2).first().getId();
        String jobId2 = catalogManager.getJobManager().submit(studyFqn, VariantAnnotationIndexOperationTool.ID, Enums.Priority.MEDIUM,
                params, "job2", "", null, null, null, null, false, orgAdminToken1).first().getId();
        String jobId3 = catalogManager.getJobManager().submit(studyFqn, VariantAnnotationIndexOperationTool.ID, Enums.Priority.MEDIUM,
                params, "job3", "", null, null, null, null, false, ownerToken).first().getId();
        String jobId4 = catalogManager.getJobManager().submit(studyFqn, VariantAnnotationIndexOperationTool.ID, Enums.Priority.MEDIUM,
                params, "job4", "", null, null, null, null, false, normalToken3).first().getId();

        daemon.checkJobs();

        // Job sent by the owner
        OpenCGAResult<Job> jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId3, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.QUEUED);

        // Job sent by normal user
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId1, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.PENDING);

        // Job sent by study administrator
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId4, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.PENDING);

        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId2, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.PENDING);

        daemon.checkJobs();

        // Because there's already a QUEUED job, it should not process any more jobs
        // Job sent by the owner
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId3, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.QUEUED);

        // Job sent normal user
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId1, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.PENDING);

        // Job sent by study administrator
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId4, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.PENDING);

        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId2, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.PENDING);

        // Set to DONE jobId3 so it can process more jobs
        catalogManager.getJobManager().update(studyFqn, jobId3, new PrivateJobUpdateParams()
                .setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE))), QueryOptions.empty(), ownerToken);

        daemon.checkJobs();
        // Job sent by normal user should still be PENDING
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId1, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.PENDING);

        // Job sent by study administrator
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId4, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.PENDING);

        // Job sent by org admin
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId2, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.QUEUED);

        // Set to DONE jobId2 so it can process more jobs
        catalogManager.getJobManager().update(studyFqn, jobId2, new PrivateJobUpdateParams()
                .setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE))), QueryOptions.empty(), ownerToken);

        daemon.checkJobs();
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId1, QueryOptions.empty(), ownerToken);
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.ABORTED);
        assertTrue(jobOpenCGAResult.first().getInternal().getStatus().getDescription()
                .contains("can only be executed by the project owners or members of " + ParamConstants.ADMINS_GROUP));

        // Job sent by study administrator
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId4, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.PENDING);

        daemon.checkJobs(); // to process jobId4

        // Job sent by study administrator
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId4, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.QUEUED);

        jobOpenCGAResult = catalogManager.getJobManager().search(studyFqn2, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(0, jobOpenCGAResult.getNumResults());

        jobOpenCGAResult = catalogManager.getJobManager().search(studyFqn2, new Query(),
                new QueryOptions(ParamConstants.OTHER_STUDIES_FLAG, true), ownerToken);
        assertEquals(3, jobOpenCGAResult.getNumResults());
    }

    @Test
    public void testDependsOnJobs() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        String job1 = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, ownerToken).first().getId();
        String job2 = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, null, null,
                Collections.singletonList(job1), null, null, null, false, ownerToken).first().getId();

        daemon.checkPendingJobs(organizationIds);

        OpenCGAResult<Job> jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, job1, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.QUEUED);

        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, job2, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.PENDING);

        // Set the status of job1 to ERROR
        catalogManager.getJobManager().update(studyFqn, job1, new PrivateJobUpdateParams()
                .setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR))), QueryOptions.empty(), ownerToken);

        // The job that depended on job1 should be ABORTED because job1 execution "failed"
        daemon.checkPendingJobs(organizationIds);
        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, job2, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.ABORTED);
        assertTrue(jobOpenCGAResult.first().getInternal().getStatus().getDescription().contains("depended on did not finish successfully"));

        // Set status of job1 to DONE to simulate it finished successfully
        catalogManager.getJobManager().update(studyFqn, job1, new PrivateJobUpdateParams()
                .setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE))), QueryOptions.empty(), ownerToken);

        // And create a new job to simulate a normal successfully dependency
        String job3 = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, null, null,
                Collections.singletonList(job1), null, null, null, false, ownerToken).first().getId();
        daemon.checkPendingJobs(organizationIds);

        jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, job3, QueryOptions.empty(), ownerToken);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        checkStatus(jobOpenCGAResult.first(), Enums.ExecutionStatus.QUEUED);
    }

    @Test
    public void testDependsOnMultiStudy() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        Job firstJob = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, ownerToken).first();
        Job job = catalogManager.getJobManager().submit(studyFqn2, "files-delete", Enums.Priority.MEDIUM, params, null, null,
                Collections.singletonList(firstJob.getUuid()), null, null, null, false, ownerToken).first();
        assertEquals(1, job.getDependsOn().size());
        assertEquals(firstJob.getId(), job.getDependsOn().get(0).getId());
        assertEquals(firstJob.getUuid(), job.getDependsOn().get(0).getUuid());

        job = catalogManager.getJobManager().get(studyFqn2, job.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(1, job.getDependsOn().size());
        assertEquals(firstJob.getId(), job.getDependsOn().get(0).getId());
        assertEquals(firstJob.getUuid(), job.getDependsOn().get(0).getUuid());
    }

    @Test
    public void testRunJob() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outDir");
        org.opencb.opencga.core.models.file.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, ownerToken).first();
        params.put("myFile", inputFile.getPath());
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, ownerToken).first();
        String jobId = job.getId();

        daemon.checkJobs();

        String[] cli = getJob(jobId).getCommandLine().split(" ");
        int i = Arrays.asList(cli).indexOf("--my-file");
        assertEquals("'" + inputFile.getPath() + "'", cli[i + 1]);
        assertEquals(1, getJob(jobId).getInput().size());
        assertEquals(inputFile.getPath(), getJob(jobId).getInput().get(0).getPath());
        checkStatus(getJob(jobId), Enums.ExecutionStatus.QUEUED);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        daemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.RUNNING);
        createAnalysisResult(jobId, "myTest", false);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.READY);

        daemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.DONE);
    }

    @Test
    public void testCheckLogs() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outDir");
        org.opencb.opencga.core.models.file.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, ownerToken).first();
        params.put("myFile", inputFile.getPath());
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, ownerToken).first();
        String jobId = job.getId();

        daemon.checkJobs();

        String[] cli = getJob(jobId).getCommandLine().split(" ");
        int i = Arrays.asList(cli).indexOf("--my-file");
        assertEquals("'" + inputFile.getPath() + "'", cli[i + 1]);
        assertEquals(1, getJob(jobId).getInput().size());
        assertEquals(inputFile.getPath(), getJob(jobId).getInput().get(0).getPath());
        checkStatus(getJob(jobId), Enums.ExecutionStatus.QUEUED);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        job = catalogManager.getJobManager().get(studyFqn, jobId, null, ownerToken).first();

        daemon.checkJobs();
        checkStatus(getJob(jobId), Enums.ExecutionStatus.RUNNING);

        InputStream inputStream = new ByteArrayInputStream("my log content\nlast line".getBytes(StandardCharsets.UTF_8));
        catalogManager.getIoManagerFactory().getDefault().copy(inputStream,
                Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".log").toUri());

        OpenCGAResult<FileContent> fileContentResult = catalogManager.getJobManager().log(studyFqn, jobId, 0, 1, "stdout", true, ownerToken);
        assertEquals("last line", fileContentResult.first().getContent());

        fileContentResult = catalogManager.getJobManager().log(studyFqn, jobId, 0, 1, "stdout", false, ownerToken);
        assertEquals("my log content\n", fileContentResult.first().getContent());
    }

    @Test
    public void testCheckLogsNoPermissions() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outDir");
        org.opencb.opencga.core.models.file.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, ownerToken).first();
        params.put("myFile", inputFile.getPath());
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, ownerToken).first();
        String jobId = job.getId();

        catalogManager.getJobManager().updateAcl(studyFqn, Collections.singletonList(jobId), normalUserId2,
                new AclParams(JobPermissions.VIEW.name()), ParamUtils.AclAction.ADD, ownerToken);

        daemon.checkJobs();

        String[] cli = getJob(jobId).getCommandLine().split(" ");
        int i = Arrays.asList(cli).indexOf("--my-file");
        assertEquals("'" + inputFile.getPath() + "'", cli[i + 1]);
        assertEquals(1, getJob(jobId).getInput().size());
        assertEquals(inputFile.getPath(), getJob(jobId).getInput().get(0).getPath());
        checkStatus(getJob(jobId), Enums.ExecutionStatus.QUEUED);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        job = catalogManager.getJobManager().get(studyFqn, jobId, null, ownerToken).first();

        daemon.checkJobs();
        checkStatus(getJob(jobId), Enums.ExecutionStatus.RUNNING);

        InputStream inputStream = new ByteArrayInputStream("my log content\nlast line".getBytes(StandardCharsets.UTF_8));
        catalogManager.getIoManagerFactory().getDefault().copy(inputStream,
                Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".log").toUri());

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("view log file");
        catalogManager.getJobManager().log(studyFqn, jobId, 0, 1, "stdout", true, normalToken2);
    }

    @Test
    public void testRegisterFilesSuccessfully() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
//        params.put(ExecutionDaemon.OUTDIR_PARAM, "outDir");
        org.opencb.opencga.core.models.file.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, ownerToken).first();
        params.put("myFile", inputFile.getPath());
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, ownerToken).first();
        String jobId = job.getId();

        daemon.checkJobs();

        String[] cli = getJob(jobId).getCommandLine().split(" ");
        int i = Arrays.asList(cli).indexOf("--my-file");
        assertEquals("'" + inputFile.getPath() + "'", cli[i + 1]);
        assertEquals(1, getJob(jobId).getInput().size());
        assertEquals(inputFile.getPath(), getJob(jobId).getInput().get(0).getPath());
        checkStatus(getJob(jobId), Enums.ExecutionStatus.QUEUED);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        daemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.RUNNING);
        createAnalysisResult(jobId, "myTest", false);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.READY);

        job = catalogManager.getJobManager().get(studyFqn, job.getId(), QueryOptions.empty(), ownerToken).first();
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("file1.txt"));
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("file2.txt"));
        Files.createDirectory(Paths.get(job.getOutDir().getUri()).resolve("A"));
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("A/file3.txt"));

        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".log"));
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".err"));

        daemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.DONE);

        job = catalogManager.getJobManager().get(studyFqn, job.getId(), QueryOptions.empty(), ownerToken).first();

        String outDir = job.getOutDir().getPath();

        assertEquals(4, job.getOutput().size());
        for (org.opencb.opencga.core.models.file.File file : job.getOutput()) {
            assertTrue(Arrays.asList(outDir + "file1.txt", outDir + "file2.txt", outDir + "A/", outDir + "A/file3.txt")
                    .contains(file.getPath()));
        }
        assertEquals(0, job.getOutput().stream().filter(f -> ExecutionResultManager.isExecutionResultFile(f.getName()))
                .collect(Collectors.toList()).size());

        assertEquals(job.getId() + ".log", job.getStdout().getName());
        assertEquals(job.getId() + ".err", job.getStderr().getName());

        // Check jobId is properly populated
        OpenCGAResult<org.opencb.opencga.core.models.file.File> files = catalogManager.getFileManager().search(studyFqn,
                new Query(FileDBAdaptor.QueryParams.JOB_ID.key(), job.getId()), FileManager.INCLUDE_FILE_URI_PATH, ownerToken);
        List<String> expectedFiles = Arrays.asList(
                outDir,
                outDir + "file1.txt",
                outDir + "file2.txt",
                outDir + "A/",
                outDir + "A/file3.txt",
                outDir + "" + job.getId() + ".log",
                outDir + "" + job.getId() + ".err");
        assertEquals(expectedFiles.size(), files.getNumResults());
        for (org.opencb.opencga.core.models.file.File file : files.getResults()) {
            assertTrue(expectedFiles.contains(file.getPath()));
            if (file.getPath().endsWith("/")) {
                assertEquals(org.opencb.opencga.core.models.file.File.Type.DIRECTORY, file.getType());
            } else {
                assertEquals(org.opencb.opencga.core.models.file.File.Type.FILE, file.getType());
            }
        }

        files = catalogManager.getFileManager().count(studyFqn, new Query(FileDBAdaptor.QueryParams.JOB_ID.key(), ""), ownerToken);
        assertEquals(16, files.getNumMatches());
        files = catalogManager.getFileManager().count(studyFqn, new Query(FileDBAdaptor.QueryParams.JOB_ID.key(), "NonE"), ownerToken);
        assertEquals(16, files.getNumMatches());
    }

    @Test
    public void scheduledJobTest() throws CatalogException, InterruptedException {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outputDir/");
        Date date = new Date();
        // Create a date object with the current time + 2 seconds
        date.setTime(date.getTime() + 2000);
        String scheduledTime = TimeUtils.getTime(date);
        System.out.println("Scheduled time: " + scheduledTime);
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, null, null, null,
                null, null, scheduledTime, null, ownerToken).first();

        daemon.checkJobs();
        checkStatus(getJob(job.getId()), Enums.ExecutionStatus.PENDING);
        daemon.checkJobs();
        checkStatus(getJob(job.getId()), Enums.ExecutionStatus.PENDING);

        // Sleep for 2 seconds and check again
        Thread.sleep(2000);
        daemon.checkJobs();
        checkStatus(getJob(job.getId()), Enums.ExecutionStatus.QUEUED);
    }

    @Test
    public void testRunJobFail() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outputDir/");
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, ownerToken).first();
        String jobId = job.getId();

        daemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.QUEUED);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        daemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.RUNNING);
        createAnalysisResult(jobId, "myTest", true);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.ERROR);

        daemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.ERROR);
        assertEquals("Job could not finish successfully", getJob(jobId).getInternal().getStatus().getDescription());
    }

    @Test
    public void testRunJobFailMissingExecutionResult() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outputDir/");
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, ownerToken).first();
        String jobId = job.getId();

        daemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.QUEUED);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        daemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.RUNNING);
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.READY);

        daemon.checkJobs();

        checkStatus(getJob(jobId), Enums.ExecutionStatus.ERROR);
        assertEquals("Job could not finish successfully. Missing execution result", getJob(jobId).getInternal().getStatus().getDescription());
    }

    @Test
    public void registerMalformedVcfFromExecutedJobTest() throws CatalogException {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outputDir/");
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, ownerToken).first();
        String jobId = job.getId();

        daemon.checkJobs();
        job = catalogManager.getJobManager().get(studyFqn, jobId, QueryOptions.empty(), ownerToken).first();
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.READY);
        try {
            // Create an empty VCF file (this will fail because OpenCGA will not be able to parse it)
            Path vcffile = Paths.get(job.getOutDir().getUri()).resolve("myemptyvcf.vcf");
            Files.createFile(vcffile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        daemon.checkJobs();

        // Check the file has been properly registered
        job = catalogManager.getJobManager().get(studyFqn, jobId, QueryOptions.empty(), ownerToken).first();
        assertEquals(1, job.getOutput().size());
        assertEquals("myemptyvcf.vcf", job.getOutput().get(0).getName());
        assertEquals(File.Format.VCF, job.getOutput().get(0).getFormat());
        assertEquals(FileStatus.ERROR, job.getOutput().get(0).getInternal().getStatus().getId());
    }

    private void checkStatus(Job job, String status) {
        assertEquals(status, job.getInternal().getStatus().getId());
        assertEquals(status, job.getInternal().getStatus().getName());
    }

    private Job getJob(String jobId) throws CatalogException {
        return catalogManager.getJobManager().get(studyFqn, jobId, new QueryOptions(), ownerToken).first();
    }

    private void createAnalysisResult(String jobId, String analysisId, boolean error) throws CatalogException, ToolException {
        ExecutionResultManager erm = new ExecutionResultManager(analysisId, Paths.get(getJob(jobId).getOutDir().getUri()));
        erm.init(new ObjectMap(), new ObjectMap(), false);
        if (error) {
            erm.close(new Exception());
        } else {
            erm.close();
        }
    }

    private static class DummyBatchExecutor implements BatchExecutor {

        public Map<String, String> jobStatus = new HashMap<>();

        @Override
        public void execute(String jobId, String queue, String commandLine, Path stdout, Path stderr) throws Exception {
            System.out.println("Executing job " + jobId + " --- " + commandLine);
            jobStatus.put(jobId, Enums.ExecutionStatus.QUEUED);
        }

        @Override
        public void close() throws IOException {
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
