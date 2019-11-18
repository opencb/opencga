package org.opencb.opencga.catalog.monitor.daemons;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.catalog.monitor.executors.BatchExecutor;
import org.opencb.opencga.core.analysis.result.AnalysisResult;
import org.opencb.opencga.core.analysis.result.AnalysisResultManager;
import org.opencb.opencga.core.analysis.result.Status;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.common.Enums;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class ExecutionDaemonTest extends AbstractManagerTest {

    private ExecutionDaemon daemon;
    private DummyBatchExecutor executor;

    @Override
    @Before
    public void setUp() throws IOException, CatalogException {
        super.setUp();

        String expiringToken = this.catalogManager.getUserManager().login("admin", "admin");
        String nonExpiringToken = this.catalogManager.getUserManager().getSystemTokenForUser("admin", expiringToken);

        daemon = new ExecutionDaemon(1000, nonExpiringToken, catalogManager, "/tmp");
        executor = new DummyBatchExecutor();
        daemon.batchExecutor = executor;
    }

    @Test
    public void testBuildCli() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("key", "value");
        params.put("camelCaseKey", "value");
        params.put("flag", "");
        params.put("boolean", "true");
        params.put("outdir", "/tmp/folder");
        params.put("-Ddynamic", "true");
        String cli = ExecutionDaemon.buildCli("opencga-internal.sh", "variant", "index", params);
        assertEquals("opencga-internal.sh variant index --key value --camel-case-key value --flag  --boolean true --outdir /tmp/folder -Ddynamic=true", cli);
    }

    @Test
    public void testCreateOutDir() throws Exception {

        HashMap<String, String> params = new HashMap<>();
        params.put("outdir", "data");
        String jobId = catalogManager.getJobManager().submit(studyFqn, "command", "subcommand", Enums.Priority.MEDIUM, params, sessionIdUser).first().getId();

        daemon.checkPendingJobs();

        URI uri = getJob(jobId).getTmpDir().getUri();
        Assert.assertTrue(Files.exists(Paths.get(uri)));
    }

    @Test
    public void testCheckPendingJobEmptyOutdir() throws Exception {
        HashMap<String, String> params = new HashMap<>();
        params.put("other", "data");
        String jobId = catalogManager.getJobManager().submit(studyFqn, "command", "subcommand", Enums.Priority.MEDIUM, params, sessionIdUser).first().getId();

        daemon.checkPendingJobs();

        assertEquals(Job.JobStatus.ABORTED, getJob(jobId).getStatus().getName());
        assertEquals("Missing mandatory outdir directory", getJob(jobId).getStatus().getMessage());
    }

    @Test
    public void testRunJob() throws Exception {
        HashMap<String, String> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "data");
        org.opencb.opencga.core.models.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, sessionIdUser).first();
        params.put("myFile", inputFile.getPath());
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant", "index", Enums.Priority.MEDIUM, params, sessionIdUser).first();
        String jobId = job.getId();

        daemon.checkJobs();

        String[] cli = getJob(jobId).getCommandLine().split(" ");
        int i = Arrays.binarySearch(cli, "--my-file");
        assertEquals(inputFile.getUri().getPath(), cli[i + 1]);
        assertEquals(1, getJob(jobId).getInput().size());
        assertEquals(inputFile.getPath(), getJob(jobId).getInput().get(0).getPath());
        assertEquals(Job.JobStatus.QUEUED, getJob(jobId).getStatus().getName());
        executor.jobStatus.put(jobId, Job.JobStatus.RUNNING);

        daemon.checkJobs();

        assertEquals(Job.JobStatus.RUNNING, getJob(jobId).getStatus().getName());
        createAnalysisResult(jobId, "myTest", ar -> ar.setStatus(new Status(Status.Type.DONE, null, null)));
        executor.jobStatus.put(jobId, Job.JobStatus.READY);

        daemon.checkJobs();

        assertEquals(Job.JobStatus.DONE, getJob(jobId).getStatus().getName());
    }

    @Test
    public void testRunJobFail() throws Exception {

        HashMap<String, String> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "data");
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant", "index", Enums.Priority.MEDIUM, params, sessionIdUser).first();
        String jobId = job.getId();

        daemon.checkJobs();

        assertEquals(Job.JobStatus.QUEUED, getJob(jobId).getStatus().getName());
        executor.jobStatus.put(jobId, Job.JobStatus.RUNNING);

        daemon.checkJobs();

        assertEquals(Job.JobStatus.RUNNING, getJob(jobId).getStatus().getName());
        createAnalysisResult(jobId, "myTest", ar -> ar.setStatus(new Status(Status.Type.ERROR, null, null)));
        executor.jobStatus.put(jobId, Job.JobStatus.ERROR);

        daemon.checkJobs();

        assertEquals(Job.JobStatus.ERROR, getJob(jobId).getStatus().getName());
        assertEquals("Job could not finish successfully", getJob(jobId).getStatus().getMessage());
    }

    @Test
    public void testRunJobFailMissingAnalysisResult() throws Exception {

        HashMap<String, String> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "data");
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant", "index", Enums.Priority.MEDIUM, params, sessionIdUser).first();
        String jobId = job.getId();

        daemon.checkJobs();

        assertEquals(Job.JobStatus.QUEUED, getJob(jobId).getStatus().getName());
        executor.jobStatus.put(jobId, Job.JobStatus.RUNNING);

        daemon.checkJobs();

        assertEquals(Job.JobStatus.RUNNING, getJob(jobId).getStatus().getName());
        executor.jobStatus.put(jobId, Job.JobStatus.READY);

        daemon.checkJobs();

        assertEquals(Job.JobStatus.ERROR, getJob(jobId).getStatus().getName());
        assertEquals("Job could not finish successfully. Missing analysis result", getJob(jobId).getStatus().getMessage());
    }

    private Job getJob(String jobId) throws CatalogException {
        return catalogManager.getJobManager().get(studyFqn, jobId, new QueryOptions(), sessionIdUser).first();
    }

    private void createAnalysisResult(String jobId, String analysisId, Consumer<AnalysisResult> c) throws CatalogException, IOException {
        AnalysisResult ar = new AnalysisResult();
        ar.setId(analysisId);
        c.accept(ar);
        File resultFile = Paths.get(getJob(jobId).getTmpDir().getUri()).resolve(analysisId + AnalysisResultManager.FILE_EXTENSION).toFile();
        JacksonUtils.getDefaultObjectMapper().writeValue(resultFile, ar);
    }

    private static class DummyBatchExecutor implements BatchExecutor {

        public Map<String, String> jobStatus = new HashMap<>();

        @Override
        public void execute(String jobId, String commandLine, Path stdout, Path stderr, String token) throws Exception {
            System.out.println("Executing job " + jobId + " --- " + commandLine);
            jobStatus.put(jobId, Job.JobStatus.QUEUED);
        }

        @Override
        public String getStatus(Job job) {
            return jobStatus.getOrDefault(job.getId(), Job.JobStatus.UNKNOWN);
        }

        @Override
        public boolean isExecutorAlive() {
            return true;
        }

        @Override
        public boolean stop(Job job) throws Exception {
            return false;
        }

        @Override
        public boolean resume(Job job) throws Exception {
            return false;
        }

        @Override
        public boolean kill(Job job) throws Exception {
            return false;
        }
    }
}