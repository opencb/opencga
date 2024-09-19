package org.opencb.opencga.analysis.workflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.utils.InputFileUtils;
import org.opencb.opencga.catalog.db.api.WorkflowDBAdaptor;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UserProcessUtils;
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

import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

@Tool(id = NextFlowExecutor.ID, resource = Enums.Resource.WORKFLOW, description = NextFlowExecutor.DESCRIPTION)
public class NextFlowExecutor extends OpenCgaToolScopeStudy {

    public final static String ID = "workflow";
    public static final String DESCRIPTION = "Execute a Nextflow analysis.";

    @ToolParams
    protected NextFlowRunParams nextflowParams = new NextFlowRunParams();

    private Workflow workflow;
    private String cliParams;
    // Build list of inputfiles in case we need to specifically mount them in read only mode
    private List<String> inputFileUris;
    // Build list of inputfiles in case we need to specifically mount them in read only mode
    List<AbstractMap.SimpleEntry<String, String>> inputBindings;

    private Thread thread;
    private final int monitorThreadPeriod = 5000;

    private final Path inputDir = Paths.get("/data/input");
    private final String outputDir = "/data/output";

    private final static Logger logger = LoggerFactory.getLogger(NextFlowExecutor.class);

    @Override
    protected void check() throws Exception {
        super.check();

        if (nextflowParams.getId() == null) {
            throw new IllegalArgumentException("Missing Nextflow ID");
        }

        OpenCGAResult<Workflow> result;
        if (nextflowParams.getVersion() != null) {
            Query query = new Query(WorkflowDBAdaptor.QueryParams.VERSION.key(), nextflowParams.getVersion());
            result = catalogManager.getWorkflowManager().get(study, Collections.singletonList(nextflowParams.getId()), query,
                    QueryOptions.empty(), false, token);
        } else {
            result = catalogManager.getWorkflowManager().get(study, nextflowParams.getId(), QueryOptions.empty(), token);
        }
        if (result.getNumResults() == 0) {
            throw new ToolException("Workflow '" + nextflowParams.getId() + "' not found");
        }
        workflow = result.first();

        if (workflow == null) {
            throw new ToolException("Workflow '" + nextflowParams.getId() + "' is null");
        }

        // Update job tags and attributes
        ObjectMap attributes = new ObjectMap()
                .append("WORKFLOW_ID", workflow.getManager().getId())
                .append("WORKFLOW_VERSION", workflow.getManager().getVersion());
        Set<String> tags = new HashSet<>();
        tags.add(ID);
        tags.add(workflow.getManager().getId().name());
        tags.add(workflow.getManager().getId() + ":" + workflow.getManager().getVersion());
        tags.add(workflow.getId());
        if (CollectionUtils.isNotEmpty(workflow.getTags())) {
            tags.addAll(workflow.getTags());
        }
        updateJobInformation(new ArrayList<>(tags), attributes);

        this.inputBindings = new LinkedList<>();
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
                    inputBindings.add(new AbstractMap.SimpleEntry<>(file.getUri().getPath(), inputDir.resolve(file.getName()).toString()));
                    cliParamsBuilder.append(inputDir).append("/").append(file.getName()).append(" ");
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
        Path temporalInputDir = Files.createDirectory(getOutDir().resolve(".input"));
        for (WorkflowScript script : workflow.getScripts()) {
            // Write script files
            Files.write(temporalInputDir.resolve(script.getFileName()), script.getContent().getBytes());
            inputBindings.add(new AbstractMap.SimpleEntry<>(temporalInputDir.resolve(script.getFileName()).toString(),
                    inputDir.resolve(script.getFileName()).toString()));
        }

        // Write nextflow.config file
        URL nextflowConfig = getClass().getResource("/nextflow.config");
        if (nextflowConfig != null) {
            Files.copy(nextflowConfig.openStream(), temporalInputDir.resolve("nextflow.config"));
            inputBindings.add(new AbstractMap.SimpleEntry<>(temporalInputDir.resolve("nextflow.config").toString(),
                    inputDir.resolve("nextflow.config").toString()));
        } else {
            throw new RuntimeException("Can't fetch nextflow.config file");
        }

        Map<String, String> dockerParams = new HashMap<>();
        dockerParams.put("user", "0:0");

        // Build output binding
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                outputDir);

        String dockerImage = "nextflow/nextflow:" + workflow.getManager().getVersion();
        StringBuilder stringBuilder = new StringBuilder()
                .append("bash -c \"nextflow -c ").append(inputDir).append("/nextflow.config").append(" run ");
        if (workflow.getRepository() != null && StringUtils.isNotEmpty(workflow.getRepository().getImage())) {
            stringBuilder.append(workflow.getRepository().getImage()).append(" -with-docker");
            dockerParams.put("-v", "/var/run/docker.sock:/var/run/docker.sock");
        } else {
            for (WorkflowScript script : workflow.getScripts()) {
                if (script.isMain()) {
                    stringBuilder.append(inputDir).append("/").append(script.getFileName());
                    break;
                }
            }
        }
        if (StringUtils.isNotEmpty(cliParams)) {
            stringBuilder.append(" ").append(cliParams);
        }
        stringBuilder.append(" -with-report ").append(outputDir).append("/report.html");
        // And give ownership permissions to the user running this process
        stringBuilder.append("; chown -R ")
                .append(UserProcessUtils.getUserUid()).append(":").append(UserProcessUtils.getGroupId()).append(" ").append(outputDir)
                .append("\"");

        startTraceFileMonitor();

        // Execute docker image
        StopWatch stopWatch = StopWatch.createStarted();
        DockerUtils.run(dockerImage, inputBindings, outputBinding, stringBuilder.toString(), dockerParams);
        logger.info("Execution time: " + TimeUtils.durationToString(stopWatch));

        // Delete input files and temporal directory
        for (WorkflowScript script : workflow.getScripts()) {
            Files.delete(temporalInputDir.resolve(script.getFileName()));
        }
        Files.delete(temporalInputDir.resolve("nextflow.config"));
        Files.delete(temporalInputDir);

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
