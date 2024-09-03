package org.opencb.opencga.analysis.workflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.utils.InputFileUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.workflow.NextFlowRunParams;
import org.opencb.opencga.core.models.workflow.Workflow;
import org.opencb.opencga.core.models.workflow.WorkflowScript;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.result.Status;
import org.opencb.opencga.core.tools.result.ToolStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

@Tool(id = NextFlowExecutor.ID, resource = Enums.Resource.WORKFLOW, description = NextFlowExecutor.DESCRIPTION)
public class NextFlowExecutor extends OpenCgaToolScopeStudy {

    public final static String ID = "nextflow-run";
    public static final String DESCRIPTION = "Execute a Nextflow analysis.";

    @ToolParams
    protected NextFlowRunParams nextflowParams = new NextFlowRunParams();

    private Workflow workflow;
    private String cliParams;
    // Build list of inputfiles in case we need to specifically mount them in read only mode
    private List<String> inputFileUris;

    private Thread thread;
    private final int monitorThreadPeriod = 5000;

    private final static Logger logger = LoggerFactory.getLogger(NextFlowExecutor.class);

    @Override
    protected void check() throws Exception {
        super.check();

        if (nextflowParams.getId() == null) {
            throw new IllegalArgumentException("Missing Nextflow ID");
        }

        OpenCGAResult<Workflow> result = catalogManager.getWorkflowManager().get(study, nextflowParams.getId(), QueryOptions.empty(), token);
        if (result.getNumResults() == 0) {
            throw new ToolException("Workflow '" + nextflowParams.getId() + "' not found");
        }
        workflow = result.first();

        if (workflow == null) {
            throw new ToolException("Workflow '" + nextflowParams.getId() + "' is null");
        }

        if (MapUtils.isNotEmpty(nextflowParams.getParams())) {
            this.inputFileUris = new LinkedList<>();
            InputFileUtils inputFileUtils = new InputFileUtils(catalogManager);

            StringBuilder cliParamsBuilder = new StringBuilder();
            for (Map.Entry<String, String> entry : nextflowParams.getParams().entrySet()) {
                if (entry.getKey().startsWith("--")) {
                    cliParamsBuilder.append(entry.getKey()).append(" ");
                } else {
                    cliParamsBuilder.append("--").append(entry.getKey()).append(" ");
                }
                if (inputFileUtils.isValidOpenCGAFile(entry.getValue())) {
                    File file = inputFileUtils.getOpenCGAFile(study, entry.getValue(), token);
                    cliParamsBuilder.append(file.getUri().getPath()).append(" ");
                    inputFileUris.add(file.getUri().getPath());
                } else {
                    cliParamsBuilder.append(entry.getValue()).append(" ");
                }
            }
            this.cliParams = cliParamsBuilder.toString();
        } else {
            this.cliParams = "";
            this.inputFileUris = Collections.emptyList();
        }
    }

    @Override
    protected void run() throws Exception {
        for (WorkflowScript script : workflow.getScripts()) {
            // Write script files
            Files.write(getOutDir().resolve(script.getFilename()), script.getContent().getBytes());
        }

        // Write nextflow.config file
        URL nextflowConfig = getClass().getResource("/nextflow.config");
        if (nextflowConfig != null) {
            Files.copy(nextflowConfig.openStream(), getOutDir().resolve("nextflow.config"));
        } else {
            throw new RuntimeException("Can't fetch nextflow.config file");
        }

        // Execute docker image
        String workingDirectory = getOutDir().toAbsolutePath().toString();

        StringBuilder stringBuilder = new StringBuilder()
                .append("nextflow -c ").append(workingDirectory).append("/nextflow.config")
                .append(" run ");
        if (workflow.getRepository() != null && StringUtils.isNotEmpty(workflow.getRepository().getImage())) {
            stringBuilder.append(workflow.getRepository().getImage()).append(" -with-docker");
        } else {
            for (WorkflowScript script : workflow.getScripts()) {
                if (script.isMain()) {
                    stringBuilder.append(workingDirectory).append("/").append(script.getFilename());
                    break;
                }
            }
        }
        if (StringUtils.isNotEmpty(cliParams)) {
            stringBuilder.append(" ").append(cliParams);
        }
        stringBuilder.append(" -with-report ").append(workingDirectory).append("/report.html");
        List<String> cliArgs = Arrays.asList(StringUtils.split(stringBuilder.toString(), " "));

        startTraceFileMonitor();

        StopWatch stopWatch = StopWatch.createStarted();

        // Execute nextflow binary
        ProcessBuilder processBuilder = new ProcessBuilder(cliArgs);
        // Establish the working directory of the process
        processBuilder.directory(getOutDir().toFile());
        logger.info("Executing: {}", stringBuilder);
        Process p;
        try {
            p = processBuilder.start();
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String line;
            while ((line = input.readLine()) != null) {
                logger.info("{}", line);
            }
            while ((line = error.readLine()) != null) {
                logger.error("{} ", line);
            }
            p.waitFor();
            input.close();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error executing cli: " + e.getMessage(), e);
        }

        logger.info("Execution time: " + TimeUtils.durationToString(stopWatch));

        // Delete input files
        for (WorkflowScript script : workflow.getScripts()) {
            Files.delete(getOutDir().resolve(script.getFilename()));
        }
        Files.delete(getOutDir().resolve("nextflow.config"));

        endTraceFileMonitor();
    }

    @Override
    protected void onShutdown() {
        super.onShutdown();
        endTraceFileMonitor();
    }

    protected void endTraceFileMonitor() {
        if (thread != null) {
            thread.interrupt();
        }
        processTraceFile();
    }

    private void startTraceFileMonitor() {
        thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(monitorThreadPeriod);
                } catch (InterruptedException e) {
                    return;
                }

                processTraceFile();
            }
        });
        thread.start();
    }

    private void processTraceFile() {
        List<ToolStep> steps = new LinkedList<>();
        // Read tabular file
        Path traceFile = getOutDir().resolve("trace.txt");
        if (Files.exists(traceFile)) {
            try (Stream<String> lines = Files.lines(traceFile)) {
                // Read line one by one
                lines.forEach(line -> {
                    if (line.startsWith("task_id")) {
                        return;
                    }
                    Trace trace = new Trace(line);
                    ToolStep toolStep = trace.toToolStep();
                    steps.add(toolStep);
                });
            } catch (Exception e) {
                logger.error("Error reading trace file: " + traceFile, e);
            }
        }
        if (CollectionUtils.isNotEmpty(steps)) {
            try {
                setManualSteps(steps);
            } catch (ToolException e) {
                logger.error("Error writing nextflow steps to ExecutionResult", e);
            }
        }
    }

    private static class Trace {
        private String taskId;
        private String hash;
        private String name;
        private String status;
        private String start;
        private String complete;
        private String cpu;
        private String peak_vmem;

        public Trace() {
        }

        public Trace(String traceLine) {
            String[] split = traceLine.split("\t");
            this.taskId = split[0];
            this.hash = split[1];
            this.name = split[2];
            this.status = split[3];
            this.start = split[4];
            this.complete = split[5];
            this.cpu = split[6];
            this.peak_vmem = split[7];
        }

        public Trace(String taskId, String hash, String name, String status, String start, String complete, String cpu, String peak_vmem) {
            this.taskId = taskId;
            this.hash = hash;
            this.name = name;
            this.status = status;
            this.start = start;
            this.complete = complete;
            this.cpu = cpu;
            this.peak_vmem = peak_vmem;
        }

        public ToolStep toToolStep() {
            Date startDate = toDate(start);
            Date completeDate = toDate(complete);
            return new ToolStep(taskId, startDate, completeDate,
                    status.equals("COMPLETED") ? Status.Type.DONE : Status.Type.ERROR, toObjectMap());
        }

        public String getTaskId() {
            return taskId;
        }

        public Trace setTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public String getHash() {
            return hash;
        }

        public Trace setHash(String hash) {
            this.hash = hash;
            return this;
        }

        public String getName() {
            return name;
        }

        public Trace setName(String name) {
            this.name = name;
            return this;
        }

        public String getStatus() {
            return status;
        }

        public Trace setStatus(String status) {
            this.status = status;
            return this;
        }

        public String getStart() {
            return start;
        }

        public Trace setStart(String start) {
            this.start = start;
            return this;
        }

        public String getComplete() {
            return complete;
        }

        public Trace setComplete(String complete) {
            this.complete = complete;
            return this;
        }

        public String getCpu() {
            return cpu;
        }

        public Trace setCpu(String cpu) {
            this.cpu = cpu;
            return this;
        }

        public String getPeak_vmem() {
            return peak_vmem;
        }

        public Trace setPeak_vmem(String peak_vmem) {
            this.peak_vmem = peak_vmem;
            return this;
        }

        public ObjectMap toObjectMap() {
            ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
            try {
                return new ObjectMap(objectMapper.writeValueAsString(this));
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static Date toDate(String dateStr) {
        String format = "yyyy-MM-dd HH:mm:ss.SSS";
        SimpleDateFormat sdf = new SimpleDateFormat(format);

        Date date = null;
        try {
            date = sdf.parse(dateStr);
        } catch (ParseException e) {
            logger.warn(e.getMessage());
        }
        return date;
    }

}
