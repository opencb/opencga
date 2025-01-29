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
import org.opencb.opencga.analysis.tools.OpenCgaDockerToolScopeStudy;
import org.opencb.opencga.catalog.db.api.WorkflowDBAdaptor;
import org.opencb.opencga.catalog.utils.InputFileUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.ToolInfoExecutor;
import org.opencb.opencga.core.models.workflow.NextFlowRunParams;
import org.opencb.opencga.core.models.workflow.Workflow;
import org.opencb.opencga.core.models.workflow.WorkflowScript;
import org.opencb.opencga.core.models.workflow.WorkflowVariable;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.ToolDependency;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.result.Status;
import org.opencb.opencga.core.tools.result.ToolStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

@Tool(id = NextFlowExecutor.ID, resource = Enums.Resource.WORKFLOW, description = NextFlowExecutor.DESCRIPTION)
public class NextFlowExecutor extends OpenCgaDockerToolScopeStudy {

    public final static String ID = "nextflow";
    public static final String DESCRIPTION = "Execute a Nextflow analysis.";

    @ToolParams
    protected NextFlowRunParams nextflowParams = new NextFlowRunParams();

    private Workflow workflow;
    private String cliParams;

    private String outDirPath;
    private String ephimeralDirPath;

    private Thread thread;
    private final int monitorThreadPeriod = 5000;

    private final static Logger logger = LoggerFactory.getLogger(NextFlowExecutor.class);

    @Override
    protected void check() throws Exception {
        super.check();

        if (nextflowParams.getId() == null) {
            throw new IllegalArgumentException("Missing Nextflow ID");
        }

        InputFileUtils inputFileUtils = new InputFileUtils(catalogManager);

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

        outDirPath = getOutDir().toAbsolutePath().toString();
        ephimeralDirPath = ephimeralDir.toAbsolutePath().toString();

        Set<String> mandatoryParams = new HashSet<>();
        Map<String, WorkflowVariable> variableMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(workflow.getVariables())) {
            for (WorkflowVariable variable : workflow.getVariables()) {
                String variableId = removePrefix(variable.getId());
                variableMap.put(variableId, variable);
                if (variable.isRequired()) {
                    mandatoryParams.add(variableId);
                }
            }
        }

        if (StringUtils.isEmpty(workflow.getManager().getVersion())) {
            workflow.getManager().setVersion(ParamConstants.DEFAULT_MIN_NEXTFLOW_VERSION);
        }
        // Update job tags and attributes
        ToolInfoExecutor toolInfoExecutor = new ToolInfoExecutor(workflow.getManager().getId().name().toLowerCase(),
                workflow.getManager().getVersion());
        Set<String> tags = new HashSet<>();
        tags.add(ID);
        tags.add(workflow.getManager().getId().name());
        tags.add(workflow.getManager().getId() + ":" + workflow.getManager().getVersion());
        tags.add(workflow.getId());
        if (CollectionUtils.isNotEmpty(workflow.getTags())) {
            tags.addAll(workflow.getTags());
        }
        List<ToolDependency> dependencyList = new ArrayList<>(4);
        dependencyList.add(new ToolDependency("opencb/opencga-workflow", "TASK-6445"));
        dependencyList.add(new ToolDependency("nextflow", workflow.getManager().getVersion()));
        dependencyList.add(new ToolDependency(workflow.getId(), String.valueOf(workflow.getVersion())));
        if (workflow.getRepository() != null && StringUtils.isNotEmpty(workflow.getRepository().getId())) {
            dependencyList.add(new ToolDependency(workflow.getRepository().getId(), workflow.getRepository().getVersion()));
        }
        addDependencies(dependencyList);
        updateJobInformation(new ArrayList<>(tags), toolInfoExecutor);

        StringBuilder cliParamsBuilder = new StringBuilder();
        if (MapUtils.isNotEmpty(nextflowParams.getParams())) {
            for (Map.Entry<String, String> entry : nextflowParams.getParams().entrySet()) {
                String variableId = removePrefix(entry.getKey());
                // Remove from the mandatoryParams set
                mandatoryParams.remove(variableId);

                WorkflowVariable workflowVariable = variableMap.get(variableId);

                if (entry.getKey().startsWith("-")) {
                    cliParamsBuilder.append(entry.getKey()).append(" ");
                } else {
                    cliParamsBuilder.append("--").append(entry.getKey()).append(" ");
                }
                if (StringUtils.isNotEmpty(entry.getValue())) {
                    if ((workflowVariable != null && workflowVariable.isOutput()) || inputFileUtils.isDynamicOutputFolder(entry.getValue())) {
                        processOutputCli(entry.getValue(), inputFileUtils, cliParamsBuilder);
                    } else {
                        processInputCli(entry.getValue(), inputFileUtils, cliParamsBuilder);
                    }
                } else if (workflowVariable != null) {
                    if (StringUtils.isNotEmpty(workflowVariable.getDefaultValue())) {
                        cliParamsBuilder.append(workflowVariable.getDefaultValue()).append(" ");
                    } else if (workflowVariable.isOutput()) {
                        processOutputCli("", inputFileUtils, cliParamsBuilder);
                    } else if (workflowVariable.isRequired() && workflowVariable.getType() != WorkflowVariable.WorkflowVariableType.FLAG) {
                        throw new ToolException("Missing value for mandatory parameter: '" + variableId + "'.");
                    }
                }
            }
        }

        for (String mandatoryParam : mandatoryParams) {
            logger.info("Processing missing mandatory param: '{}'", mandatoryParam);
            WorkflowVariable workflowVariable = variableMap.get(mandatoryParam);

            if (workflowVariable.getId().startsWith("-")) {
                cliParamsBuilder.append(workflowVariable.getId()).append(" ");
            } else {
                cliParamsBuilder.append("--").append(workflowVariable.getId()).append(" ");
            }

            if (StringUtils.isNotEmpty(workflowVariable.getDefaultValue())) {
                if (workflowVariable.isOutput()) {
                    processOutputCli(workflowVariable.getDefaultValue(), inputFileUtils, cliParamsBuilder);
                } else {
                    processInputCli(workflowVariable.getDefaultValue(), inputFileUtils, cliParamsBuilder);
                }
            } else if (workflowVariable.isOutput()) {
                processOutputCli("", inputFileUtils, cliParamsBuilder);
            } else {
                throw new ToolException("Missing mandatory parameter: '" + mandatoryParam + "'.");
            }
        }

        this.cliParams = cliParamsBuilder.toString();
    }

    @Override
    protected void run() throws Exception {
        for (WorkflowScript script : workflow.getScripts()) {
            // Write script files
            Path path = temporalInputDir.resolve(script.getFileName());
            Files.write(path, script.getContent().getBytes());
            dockerInputBindings.add(new AbstractMap.SimpleEntry<>(path.toString(), path.toString()));
        }

        // Write nextflow.config file
        URL nextflowConfig = getClass().getResource("/nextflow.config");
        Path nextflowConfigPath;
        if (nextflowConfig != null) {
            nextflowConfigPath = temporalInputDir.resolve("nextflow.config");
            writeNextflowConfigFile(nextflowConfig, nextflowConfigPath, outDirPath);
            dockerInputBindings.add(new AbstractMap.SimpleEntry<>(nextflowConfigPath.toString(), nextflowConfigPath.toString()));
        } else {
            throw new ToolException("Can't fetch nextflow.config file");
        }

        // Build output binding with output and ephimeral out directories
        List<AbstractMap.SimpleEntry<String, String>> outputBindings = new ArrayList<>(2);
        outputBindings.add(new AbstractMap.SimpleEntry<>(outDirPath, outDirPath));
        outputBindings.add(new AbstractMap.SimpleEntry<>(ephimeralDirPath, ephimeralDirPath));

        String dockerImage = "opencb/opencga-workflow:TASK-6445";
        StringBuilder stringBuilder = new StringBuilder()
                .append("bash -c \"NXF_VER=")
                .append(workflow.getManager().getVersion())
                .append(" nextflow -c ")
                .append(nextflowConfigPath)
                .append(" run ");
        if (workflow.getRepository() != null && StringUtils.isNotEmpty(workflow.getRepository().getId())) {
//            stringBuilder.append(workflow.getRepository().getImage()).append(" -with-docker");
            stringBuilder.append(workflow.getRepository().getId());
        } else {
            for (WorkflowScript script : workflow.getScripts()) {
                if (script.isMain()) {
                    stringBuilder.append(temporalInputDir.resolve(script.getFileName()));
                    break;
                }
            }
        }
        if (StringUtils.isNotEmpty(cliParams)) {
            stringBuilder.append(" ").append(cliParams);
        }
        stringBuilder.append(" -with-report ").append(outDirPath).append("/report.html\"");

        startTraceFileMonitor();

        Map<String, String> dockerParams = new HashMap<>();
        // Set HOME environment variable to the temporal input directory. This is because nextflow creates a hidden folder there and,
        // when nextflow runs on other dockers, we need to store those files in a path shared between the parent docker and the host
        // TODO: Temporal solution. We should be able to add multiple "-e" parameters
        dockerParams.put("-e", "HOME=" + ephimeralDirPath + " -e OPENCGA_TOKEN=" + getExpiringToken());
        dockerParams.put("-w", ephimeralDirPath);

        // Set user uid and guid to 1001
        dockerParams.put("user", "1001:1001");

        // Execute docker image
        StopWatch stopWatch = StopWatch.createStarted();
        runDocker(dockerImage, outputBindings, stringBuilder.toString(), dockerParams);
        logger.info("Execution time: " + TimeUtils.durationToString(stopWatch));
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }

    @Override
    protected void close() {
        super.close();
        endTraceFileMonitor();
    }

    private void writeNextflowConfigFile(URL inputUrl, Path outputFile, String outdirPath) throws ToolException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputUrl.openStream()));
             BufferedWriter writer = Files.newBufferedWriter(outputFile)) {

            String line;
            while ((line = reader.readLine()) != null) {
                // Replace occurrences of "$OUTPUT" with the replacement string
                line = line.replace("$OUTPUT", outdirPath);
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new ToolException("Could not replace 'nextflow.config' file contents", e);
        }
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
