package org.opencb.opencga.master.monitor.daemons;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;
import org.opencb.opencga.core.tools.result.Status;
import org.opencb.opencga.master.monitor.executors.BatchExecutor;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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

        String expiringToken = this.catalogManager.getUserManager().loginAsAdmin("admin");
        String nonExpiringToken = this.catalogManager.getUserManager().getNonExpiringToken("opencga", expiringToken);
        catalogManager.getConfiguration().getAnalysis().getIndex().getVariant().setMaxConcurrentJobs(1);

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
        params.put("other", Collections.singletonMap("dynamic", "true"));
        String cli = ExecutionDaemon.buildCli("opencga-internal.sh", "variant-index", params);
        assertEquals("opencga-internal.sh variant index --key value --camel-case-key value --flag  --boolean true --outdir /tmp/folder -Ddynamic=true", cli);
    }

    @Test
    public void testCreateDefaultOutDir() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, sessionIdUser).first().getId();

        daemon.checkPendingJobs();

        URI uri = getJob(jobId).getOutDir().getUri();
        Assert.assertTrue(Files.exists(Paths.get(uri)));
        assertTrue(uri.getPath().startsWith(catalogManager.getConfiguration().getJobDir()) && uri.getPath().endsWith(jobId + "/"));
    }

    @Test
    public void testCreateOutDir() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put("outdir", "outputDir/");
        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, sessionIdUser).first().getId();

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
        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params,
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
        String jobId = catalogManager.getJobManager().submit(studyFqn, "files-delete", Enums.Priority.MEDIUM, params, sessionIdUser).first().getId();

        daemon.checkPendingJobs();

        OpenCGAResult<Job> jobOpenCGAResult = catalogManager.getJobManager().get(studyFqn, jobId, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, jobOpenCGAResult.getNumResults());
        assertEquals(Enums.ExecutionStatus.ABORTED, jobOpenCGAResult.first().getStatus().getName());
        assertTrue(jobOpenCGAResult.first().getStatus().getMessage().contains("not an empty directory"));
    }

    @Test
    public void testRunJob() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outDir");
        org.opencb.opencga.core.models.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, sessionIdUser).first();
        params.put("myFile", inputFile.getPath());
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, sessionIdUser).first();
        String jobId = job.getId();

        daemon.checkJobs();

        String[] cli = getJob(jobId).getCommandLine().split(" ");
        int i = Arrays.asList(cli).indexOf("--my-file");
        assertEquals(inputFile.getPath(), cli[i + 1]);
        assertEquals(1, getJob(jobId).getInput().size());
        assertEquals(inputFile.getPath(), getJob(jobId).getInput().get(0).getPath());
        assertEquals(Enums.ExecutionStatus.QUEUED, getJob(jobId).getStatus().getName());
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        daemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.RUNNING, getJob(jobId).getStatus().getName());
        createAnalysisResult(jobId, "myTest", ar -> ar.setStatus(new Status(Status.Type.DONE, null, null)));
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.READY);

        daemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.DONE, getJob(jobId).getStatus().getName());
    }

    @Test
    public void testRegisterFilesSuccessfully() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outDir");
        org.opencb.opencga.core.models.File inputFile = catalogManager.getFileManager().get(studyFqn, testFile1, null, sessionIdUser).first();
        params.put("myFile", inputFile.getPath());
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, sessionIdUser).first();
        String jobId = job.getId();

        daemon.checkJobs();

        String[] cli = getJob(jobId).getCommandLine().split(" ");
        int i = Arrays.asList(cli).indexOf("--my-file");
        assertEquals(inputFile.getPath(), cli[i + 1]);
        assertEquals(1, getJob(jobId).getInput().size());
        assertEquals(inputFile.getPath(), getJob(jobId).getInput().get(0).getPath());
        assertEquals(Enums.ExecutionStatus.QUEUED, getJob(jobId).getStatus().getName());
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        daemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.RUNNING, getJob(jobId).getStatus().getName());
        createAnalysisResult(jobId, "myTest", ar -> ar.setStatus(new Status(Status.Type.DONE, null, null)));
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.READY);

        job = catalogManager.getJobManager().get(studyFqn, job.getId(), QueryOptions.empty(), sessionIdUser).first();
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("file1.txt"));
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("file2.txt"));
        Files.createDirectory(Paths.get(job.getOutDir().getUri()).resolve("A"));
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve("A/file3.txt"));

        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".log"));
        Files.createFile(Paths.get(job.getOutDir().getUri()).resolve(job.getId() + ".err"));

        daemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.DONE, getJob(jobId).getStatus().getName());

        job = catalogManager.getJobManager().get(studyFqn, job.getId(), QueryOptions.empty(), sessionIdUser).first();

        assertEquals(4, job.getOutput().size());
        for (org.opencb.opencga.core.models.File file : job.getOutput()) {
            assertTrue(Arrays.asList("outDir/file1.txt", "outDir/file2.txt", "outDir/A/", "outDir/A/file3.txt").contains(file.getPath()));
        }
        assertEquals(0, job.getOutput().stream().filter(f -> f.getName().endsWith(ExecutionResultManager.FILE_EXTENSION))
                .collect(Collectors.toList()).size());

        assertEquals(job.getId() + ".log", job.getStdout().getName());
        assertEquals(job.getId() + ".err", job.getStderr().getName());
    }

    @Test
    public void testRunJobFail() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outputDir/");
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, sessionIdUser).first();
        String jobId = job.getId();

        daemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.QUEUED, getJob(jobId).getStatus().getName());
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        daemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.RUNNING, getJob(jobId).getStatus().getName());
        createAnalysisResult(jobId, "myTest", ar -> ar.setStatus(new Status(Status.Type.ERROR, null, null)));
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.ERROR);

        daemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.ERROR, getJob(jobId).getStatus().getName());
        assertEquals("Job could not finish successfully", getJob(jobId).getStatus().getMessage());
    }

    @Test
    public void testRunJobFailMissingExecutionResult() throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ExecutionDaemon.OUTDIR_PARAM, "outputDir/");
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, params, sessionIdUser).first();
        String jobId = job.getId();

        daemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.QUEUED, getJob(jobId).getStatus().getName());
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);

        daemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.RUNNING, getJob(jobId).getStatus().getName());
        executor.jobStatus.put(jobId, Enums.ExecutionStatus.READY);

        daemon.checkJobs();

        assertEquals(Enums.ExecutionStatus.ERROR, getJob(jobId).getStatus().getName());
        assertEquals("Job could not finish successfully. Missing execution result", getJob(jobId).getStatus().getMessage());
    }

    private Job getJob(String jobId) throws CatalogException {
        return catalogManager.getJobManager().get(studyFqn, jobId, new QueryOptions(), sessionIdUser).first();
    }

    private void createAnalysisResult(String jobId, String analysisId, Consumer<ExecutionResult> c) throws CatalogException, IOException {
        ExecutionResult ar = new ExecutionResult();
        c.accept(ar);
        File resultFile = Paths.get(getJob(jobId).getOutDir().getUri()).resolve(analysisId + ExecutionResultManager.FILE_EXTENSION).toFile();
        JacksonUtils.getDefaultObjectMapper().writeValue(resultFile, ar);
    }

    private static class DummyBatchExecutor implements BatchExecutor {

        public Map<String, String> jobStatus = new HashMap<>();

        @Override
        public void execute(String jobId, String commandLine, Path stdout, Path stderr) throws Exception {
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
