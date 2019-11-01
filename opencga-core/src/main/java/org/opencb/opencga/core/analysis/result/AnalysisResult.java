package org.opencb.opencga.core.analysis.result;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class AnalysisResult {

    private String id;
    private ExecutorInfo executor;
    private Date start;
    private Date end;
    private Status status;
    private List<FileResult> outputFiles;
    private List<AnalysisStep> steps;
    private List<Event> events;

    private ObjectMap params;
    private ObjectMap attributes;

    private static final String DEFAULT_ANALYSIS_RESULT_FORMAT = "yaml";

    public AnalysisResult() {
        executor = new ExecutorInfo();
        status = new Status();
        events = new LinkedList<>();
        outputFiles = new LinkedList<>();
        steps = new LinkedList<>();
        attributes = new ObjectMap();
    }

    public static AnalysisResult load(Path resultPath) throws IOException {
        InputStream inputStream = FileUtils.newInputStream(resultPath);
        return load(inputStream, DEFAULT_ANALYSIS_RESULT_FORMAT);
    }

    public static AnalysisResult load(InputStream resultInputStream) throws IOException {
        return load(resultInputStream, DEFAULT_ANALYSIS_RESULT_FORMAT);
    }

    public static AnalysisResult load(InputStream resultInputStream, String format) throws IOException {
        if (resultInputStream == null) {
            throw new IOException("AnalysisResult file not found");
        }
        AnalysisResult analysisResult;
        ObjectMapper objectMapper;
        try {
            switch (format) {
                case "json":
                    objectMapper = new ObjectMapper();
                    analysisResult = objectMapper.readValue(resultInputStream, AnalysisResult.class);
                    break;
                case "yml":
                case "yaml":
                default:
                    objectMapper = new ObjectMapper(new YAMLFactory());
                    analysisResult = objectMapper.readValue(resultInputStream, AnalysisResult.class);
                    break;
            }
        } catch (IOException e) {
            throw new IOException("AnalysisResult file could not be parsed: " + e.getMessage(), e);
        }

        return analysisResult;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnalysisResult{");
        sb.append("id='").append(id).append('\'');
        sb.append(", executor=").append(executor);
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", status=").append(status);
        sb.append(", outputFiles=").append(outputFiles);
        sb.append(", steps=").append(steps);
        sb.append(", events=").append(events);
        sb.append(", params=").append(params);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public AnalysisResult setId(String id) {
        this.id = id;
        return this;
    }

    public ExecutorInfo getExecutor() {
        return executor;
    }

    public AnalysisResult setExecutor(ExecutorInfo executor) {
        this.executor = executor;
        return this;
    }

    public Date getStart() {
        return start;
    }

    public AnalysisResult setStart(Date start) {
        this.start = start;
        return this;
    }

    public Date getEnd() {
        return end;
    }

    public AnalysisResult setEnd(Date end) {
        this.end = end;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public AnalysisResult setStatus(Status status) {
        this.status = status;
        return this;
    }

    public List<Event> getEvents() {
        return events;
    }

    public AnalysisResult setEvents(List<Event> events) {
        this.events = events;
        return this;
    }

    public List<FileResult> getOutputFiles() {
        return outputFiles;
    }

    public AnalysisResult setOutputFiles(List<FileResult> outputFiles) {
        this.outputFiles = outputFiles;
        return this;
    }

    public List<AnalysisStep> getSteps() {
        return steps;
    }

    public AnalysisResult setSteps(List<AnalysisStep> steps) {
        this.steps = steps;
        return this;
    }

    public ObjectMap getParams() {
        return params;
    }

    public AnalysisResult setParams(ObjectMap params) {
        this.params = params;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public AnalysisResult setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }
}
