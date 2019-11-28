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
import org.opencb.opencga.core.results.OpenCGAResult;

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
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        Map<String, Object> params = new LinkedHashMap<>();
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
    public void testCreateDefaultOutDir() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        String jobId = catalogManager.getJobManager().submit(studyFqn, "command", "subcommand", Enums.Priority.MEDIUM, params, sessionIdUser).first().getId();

        daemon.checkPendingJobs();

        URI uri = getJob(jobId).getOutDir().getUri();
        Assert.assertTrue(Files.exists(Paths.get(uri)));
        assertTrue(uri.getPath().startsWith("/tmp/opencga/jobs/") && uri.getPath().endsWith(jobId + "/"));
    }

    @Test
    public void testCreateOutDir() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put("outdir", "outputDir/");
        String jobId = catalogManager.getJobManager().submit(studyFqn, "command", "subcommand", Enums.Priority.MEDIUM, params, sessionIdUser).first().getId();

        daemon.checkPendingJobs();

        URI uri = getJob(jobId).getOutDir().getUri();
        Assert.assertTrue(Files.exists(Paths.get(uri)));
        assertTrue(!uri.getPath().startsWith("/tmp/opencga/jobs/") && uri.getPath().endsWith("outputDir/"));
    }

    @Test
    public void testUseEmptyDirectory() throws Exception {
        // Create empty directory that is registered in OpenCGA
        org.opencb.opencga.core.models.File directory = catalogManager.getFileManager().createFolder(studyFqn, "outputDir/",
                new org.opencb.opencga.core.models.File.FileStatus(), true, "", QueryOptions.empty(), sessionIdUser).first();
        catalogManager.getCatalogIOManagerFactory().get(directory.getUri()).createDirectory(directory.getUri(), true);

        HashMap<String, Object> params = new HashMap<>();
        params.put("outdir", "outputDir/");
        String jobId = catalogManager.getJobManager().submit(studyFqn, "command", "subcommand", Enums.Priority.MEDIUM, params,
                sessionIdUser).first().getId();

        daemon.checkPendingJobs();

        URI uri = getJob(jobId).getOutDir().getUri();
        Assert.assertTrue(Files.exists(Paths.get(uri)));
        assertTrue(!uri.getPath().startsWith("/tmp/opencga/jobs/") && uri.getPath().endsWith("outputDir/"));
    }

    @Test
    public void testNotEmptyOutDir() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put("outdir", "data/");
        String jobId = catalogManager.getJobManager().submit(studyFqn, "command", "subcommand", Enums.Priority.MEDIUM, params, sessionIdUser).first().getId();

        daemon.checkPendingJobs();

        OpenCGAResult<Job> jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        assertEquals(Job.JobStatus.ABORTED, jobOpenCGAResult.first().getStatus().getName());
        assertTrue(jobOpenCGAResult.first().getStatus().getMessage().contains("not an empty directory"));
    }

    @Test
    public void testRunJob() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outDir");
        org.opencb.opencga.core.models.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, sessionIdUser).first();
        params.put("myFile", inputFile.getPath());
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant", "index", Enums.Priority.MEDIUM, params, sessionIdUser).first();
        String jobId = job.getId();

        daemon.checkJobs();

        String[] cli = getJob(jobId).getCommandLine().split(" ");
        int i = Arrays.asList(cli).indexOf("--my-file");
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
    public void testRegisterFilesSuccessfully() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outDir");
        org.opencb.opencga.core.models.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, sessionIdUser).first();
        params.put("myFile", inputFile.getPath());
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant", "index", Enums.Priority.MEDIUM, params, sessionIdUser).first();
        String jobId = job.getId();

        daemon.checkJobs();

        String[] cli = getJob(jobId).getCommandLine().split(" ");
        int i = Arrays.asList(cli).indexOf("--my-file");
        assertEquals(inputFile.getUri().getPath(), cli[i + 1]);
        assertEquals(1, getJob(jobId).getInput().size());
        assertEquals(inputFile.getPath(), getJob(jobId).getInput().get(0).getPath());
        assertEquals(Job.JobStatus.QUEUED, getJob(jobId).getStatus().getName());
        executor.jobStatus.put(jobId, Job.JobStatus.RUNNING);

        daemon.checkJobs();

        assertEquals(Job.JobStatus.RUNNING, getJob(jobId).getStatus().getName());
        createAnalysisResult(jobId, "myTest", ar -> ar.setStatus(new Status(Status.Type.DONE, null, null)));
        executor.jobStatus.put(jobId, Job.JobStatus.READY);

        job = catalogManager.getJobManager().get(studyFqn, job.getId(), QueryOptions.empty(), sessionIdUser).first();
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("file1.txt"));
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("file2.txt"));
        Files.createDirectory(Paths.get(job.getOutDir().getUri()).resolve("A"));
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("A/file3.txt"));

        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".log"));
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".err"));

        daemon.checkJobs();

        assertEquals(Job.JobStatus.DONE, getJob(jobId).getStatus().getName());

        job = catalogManager.getJobManager().get(studyFqn, job.getId(), QueryOptions.empty(), sessionIdUser).first();

        assertEquals(4, job.getOutput().size());
        for (org.opencb.opencga.core.models.File file : job.getOutput()) {
            assertTrue(Arrays.asList("outDir/file1.txt", "outDir/file2.txt", "outDir/A/", "outDir/A/file3.txt").contains(file.getPath()));
        }
        assertEquals(0, job.getOutput().stream().filter(f -> f.getName().endsWith(AnalysisResultManager.FILE_EXTENSION))
                .collect(Collectors.toList()).size());

        assertEquals(job.getId() + ".log", job.getLog().getName());
        assertEquals(job.getId() + ".err", job.getErrorLog().getName());
    }

    @Test
    public void testRunJobFail() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outputDir/");
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
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outputDir/");
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
        File resultFile = Paths.get(getJob(jobId).getOutDir().getUri()).resolve(analysisId + AnalysisResultManager.FILE_EXTENSION).toFile();
        JacksonUtils.getDefaultObjectMapper().writeValue(resultFile, ar);
    }

    private static class DummyBatchExecutor implements BatchExecutor {

        public Map<String, String> jobStatus = new HashMap<>();

        @Override
        public void execute(String jobId, String commandLine, Path stdout, Path stderr) throws Exception {
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