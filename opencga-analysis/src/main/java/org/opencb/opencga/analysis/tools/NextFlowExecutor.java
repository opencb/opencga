package org.opencb.opencga.analysis.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.nextflow.NextFlowRunParams;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

import static org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor.DOCKER_INPUT_PATH;
import static org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor.DOCKER_OUTPUT_PATH;

@Tool(id = NextFlowExecutor.ID, resource = Enums.Resource.NEXTFLOW, description = NextFlowExecutor.DESCRIPTION)
public class NextFlowExecutor extends OpenCgaTool {

    public final static String ID = "nextflow-run";
    public static final String DESCRIPTION = "Execute a Nextflow analysis.";

    public final static String DOCKER_IMAGE = "nextflow/nextflow";

    @ToolParams
    protected NextFlowRunParams toolParams = new NextFlowRunParams();

    private String script;

    private final static Logger logger = LoggerFactory.getLogger(NextFlowExecutor.class);

    @Override
    protected void check() throws Exception {
        super.check();

        if (toolParams.getId() == null) {
            throw new IllegalArgumentException("Missing Nextflow ID");
        }

        // TODO: Change and look for Nextflow script
        this.script = "params.str = 'Hello world!'\n" +
                "\n" +
                "process splitLetters {\n" +
                "    output:\n" +
                "    path 'chunk_*'\n" +
                "\n" +
                "    \"\"\"\n" +
                "    printf '${params.str}' | split -b 6 - chunk_\n" +
                "    \"\"\"\n" +
                "}\n" +
                "\n" +
                "process convertToUpper {\n" +
                "    input:\n" +
                "    path x\n" +
                "\n" +
                "    output:\n" +
                "    stdout\n" +
                "\n" +
                "    \"\"\"\n" +
                "    cat $x | tr '[a-z]' '[A-Z]'\n" +
                "    \"\"\"\n" +
                "}\n" +
                "\n" +
                "process sleep {\n" +
                "    input:\n" +
                "    val x\n" +
                "\n" +
                "    \"\"\"\n" +
                "    sleep 1\n" +
                "    \"\"\"\n" +
                "}\n" +
                "\n" +
                "workflow {\n" +
                "    splitLetters | flatten | convertToUpper | view { it.trim() } | sleep\n" +
                "}";
    }

    @Override
    protected void run() throws Exception {
        // Write main script file
        Files.write(getOutDir().resolve("pipeline.nf"), script.getBytes());

        // Write nextflow.config file
        URL nextflowConfig = getClass().getResource("/nextflow.config");
        if (nextflowConfig != null) {
            Files.copy(nextflowConfig.openStream(), getOutDir().resolve("nextflow.config"));
        } else {
            throw new RuntimeException("Can't fetch nextflow.config file");
        }

        // Execute docker image
        String workingDirectory = getOutDir().toAbsolutePath().toString();
        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
        inputBindings.add(new AbstractMap.SimpleEntry<>(workingDirectory, DOCKER_INPUT_PATH));
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(workingDirectory, DOCKER_OUTPUT_PATH);

        // TODO: Copy nextflow.config and pipeline.nf to DOCKER_INPUT_PATH

        StringBuilder stringBuilder = new StringBuilder()
                .append("nextflow run ").append(DOCKER_OUTPUT_PATH).append("/pipeline.nf")
                .append(" --work-dir ").append(DOCKER_OUTPUT_PATH)
                .append(" -with-report ").append(DOCKER_OUTPUT_PATH).append("/report.html");

        StopWatch stopWatch = StopWatch.createStarted();
        Map<String, String> dockerParams = new HashMap<>();
        dockerParams.put("user", "root");
        String cmdline = DockerUtils.run(DOCKER_IMAGE, inputBindings, outputBinding, stringBuilder.toString(), dockerParams);
        logger.info("Docker command line: " + cmdline);
        logger.info("Execution time: " + TimeUtils.durationToString(stopWatch));

        // Delete input files
        Files.delete(getOutDir().resolve("pipeline.nf"));
        Files.delete(getOutDir().resolve("nextflow.config"));

        processTraceFile();
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
                    System.out.println(trace);
                });
            } catch (Exception e) {
                logger.error("Error reading trace file: " + traceFile, e);
            }
        }

        try {
            setManualSteps(steps);
        } catch (ToolException e) {
            throw new RuntimeException(e);
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
