package org.opencb.opencga.core.tools.result;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class ExecutorResultManager {
    public static final String FILE_EXTENSION = ".result.json";
    public static final String SWAP_FILE_EXTENSION = ".swap" + FILE_EXTENSION;

    private final Path outDir;
    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;

    private Thread thread;
    private File file;
    private File swapFile;
    private boolean initialized;
    private boolean closed;
    private final Logger logger = LoggerFactory.getLogger(ExecutorResultManager.class);
    private int monitorThreadPeriod = 60000;

    public ExecutorResultManager(String toolId, Path outDir) throws ToolException {
        this.outDir = outDir.toAbsolutePath();
        ObjectMapper objectMapper = new ObjectMapper();
//        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectWriter = objectMapper.writerFor(ExecutionResult.class).withDefaultPrettyPrinter();
        objectReader = objectMapper.readerFor(ExecutionResult.class);
        initialized = false;
        closed = false;

        file = outDir.toFile();

        if (!file.exists()) {
            throw new ToolException("Output directory '" + outDir + "' does not exist");
        }
        if (!file.isDirectory()) {
            throw new ToolException("Output directory '" + outDir + "' does not a directory");
        }
        if (!file.canWrite()) {
            throw new ToolException("Write permission denied for output directory '" + outDir + "'");
        }
        if (!StringUtils.isAlphanumeric(toolId.replaceAll("[-_]", ""))) {
            throw new ToolException("Invalid ToolId. The tool id can only contain alphanumeric characters, ',' and '_'.");
        }

        file = outDir.resolve(toolId + FILE_EXTENSION).toFile();
        swapFile = outDir.resolve(toolId + SWAP_FILE_EXTENSION).toFile();
    }

    public synchronized ExecutorResultManager init(ObjectMap params, ObjectMap executorParams) throws ToolException {
        if (initialized) {
            throw new ToolException(getClass().getName() + " already initialized!");
        }
        initialized = true;

        Date now = now();
        ExecutionResult execution = new ExecutionResult()
                .setParams(params)
                .setExecutor(new ExecutorInfo()
                        .setId(executorParams.getString(OpenCgaToolExecutor.EXECUTOR_ID))
                        .setParams(executorParams))
                .setStart(now);
        execution.getStatus()
                .setDate(now)
                .setName(Status.Type.RUNNING);

        write(execution);
        startMonitorThread();
        return this;
    }

    public ExecutorResultManager setMonitorThreadPeriod(int monitorThreadPeriod) {
        this.monitorThreadPeriod = monitorThreadPeriod;
        return this;
    }

    public void setSteps(List<String> steps) throws ToolException {
        updateResult(executionResult -> {
            executionResult.setSteps(new ArrayList<>(steps.size()));
            for (String step : steps) {
                executionResult.getSteps().add(new ToolStep(step, null, null, Status.Type.PENDING, new ObjectMap()));
            }
            return null;
        });
    }

    public boolean isClosed() {
        return closed;
    }

    public synchronized ExecutionResult close() throws ToolException {
        return close(null);
    }

    public synchronized ExecutionResult close(Exception exception) throws ToolException {
        if (closed) {
            throw new ToolException(getClass().getName() + " already closed!");
        }
        thread.interrupt();

        ExecutionResult execution = read();

        Date now = now();
        execution.setEnd(now);
        execution.getStatus()
                .setDate(now);

        ToolStep step;
        if (StringUtils.isEmpty(execution.getStatus().getStep())) {
            if (CollectionUtils.isEmpty(execution.getSteps())) {
                execution.setSteps(Collections.singletonList(new ToolStep().setId("check")));
            }
            step = execution.getSteps().get(0);
            step.setStart(execution.getStart());
        } else {
            step = getStep(execution, execution.getStatus().getStep());
        }

        Status.Type finalStatus;
        if (exception == null) {
            finalStatus = Status.Type.DONE;
            for (Event event : execution.getEvents()) {
                if (event.getType().equals(Event.Type.ERROR)) {
                    // If there is any ERROR event the final status will be ERROR
                    finalStatus = Status.Type.ERROR;
                    break;
                }
            }
        } else {
            addError(exception, execution);
            finalStatus = Status.Type.ERROR;
        }

        execution.getStatus()
                .setStep(null)
                .setName(finalStatus);

        if (Status.Type.RUNNING.equals(step.getStatus()) || Status.Type.PENDING.equals(step.getStatus())) {
            step.setStatus(finalStatus);
            step.setEnd(now);
        }

        write(execution);
        closed = true;
        return execution;
    }

    public void setExecutorInfo(ExecutorInfo executorInfo) throws ToolException {
        updateResult(result -> result.setExecutor(executorInfo));
    }

    public void addWarning(String warningMessage) throws ToolException {
        updateResult(result -> result.getEvents().add(new Event(Event.Type.WARNING, warningMessage)));
    }

    public void addError(Exception exception) throws ToolException {
        updateResult(result -> addError(exception, result));
    }

    private boolean addError(Exception exception, ExecutionResult execution) {
        return execution.getEvents().add(new Event(Event.Type.ERROR, exception.getMessage()));
    }

    public void addAttribute(String key, Object value) throws ToolException {
        updateResult(result -> result.getAttributes().put(key, value));
    }

    public void addStepAttribute(String key, Object value) throws ToolException {
        updateResult(result -> {
            ToolStep step;
            if (StringUtils.isEmpty(result.getStatus().getStep())) {
                step = result.getSteps().get(0);
            } else {
                step = getStep(result, result.getStatus().getStep());
            }
            return step.getAttributes().put(key, value);
        });
    }

    public void addFile(Path file, FileResult.FileType fileType) throws ToolException {
        String fileStr = file.toAbsolutePath().toString();
        if (!file.toFile().exists()) {
            throw new ToolException("No such file or directory: " + fileStr);
        }
        String outDirStr = outDir.toString();
        String finalFileStr;
        if (fileStr.startsWith(outDirStr)) {
            fileStr = fileStr.substring(outDirStr.length());
        }
        if (fileStr.startsWith("/")) {
            fileStr = fileStr.substring(1);
        }
        finalFileStr = fileStr;
        updateResult(result -> result.getOutputFiles().add(new FileResult(finalFileStr, fileType)));
    }

    public void errorStep() throws ToolException {
        updateResult(result -> getStep(result, result.getStatus().getStep())
                .setStatus(Status.Type.ERROR).setEnd(now()));
    }

    public boolean checkStep(String stepId) throws ToolException {
        return updateResult(result -> {

            if (StringUtils.isNotEmpty(result.getStatus().getStep())) {
                // End previous step

                ToolStep step = getStep(result, result.getStatus().getStep());
                if (step.getStatus().equals(Status.Type.RUNNING)) {
                    step.setStatus(Status.Type.DONE);
                    step.setEnd(now());
                }
            }

            result.getStatus().setStep(stepId);
            ToolStep step = getStep(result, stepId);
            if (step.getStatus().equals(Status.Type.DONE)) {
                return false;
            } else {
                step.setStatus(Status.Type.RUNNING);
                step.setStart(now());
                return true;
            }
        });
    }

    private ToolStep getStep(ExecutionResult execution, String stepId) throws ToolException {
        for (ToolStep step : execution.getSteps()) {
            if (step.getId().equals(stepId)) {
                return step;
            }
        }

        List<String> steps = execution.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList());

        throw new ToolException("Step '" + stepId + "' not found. Available steps: " + steps);
    }

    public void setParams(ObjectMap params) throws ToolException {
        updateResult(result -> result.setParams(params));
    }

    private void updateStatusDate() throws ToolException {
        updateResult(result -> result.getStatus().setDate(now()));
    }

    @FunctionalInterface
    public interface ExecutionResultFunction<R> {
        R apply(ExecutionResult execution) throws ToolException;
    }

    private synchronized <R> R updateResult(ExecutionResultFunction<R> update) throws ToolException {
        ExecutionResult execution = read();
        R apply = update.apply(execution);
        write(execution);
        return apply;
    }

    public ExecutionResult read() throws ToolException {
        try {
            return objectReader.readValue(file);
        } catch (IOException e) {
            throw new ToolException("Error reading ExecutionResult", e);
        }

    }

    private synchronized void write(ExecutionResult execution) throws ToolException {
        int maxAttempts = 3;
        int attempts = 0;
        while (attempts < maxAttempts) {
            attempts++;
            try {
                if (attempts < maxAttempts) {
                    // Perform atomic writes using an intermediate temporary swap file
                    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(swapFile))) {
                        objectWriter.writeValue(os, execution);
                    }
                    Files.move(swapFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } else {
                    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
                        objectWriter.writeValue(os, execution);
                    }
                }
            } catch (IOException e) {
                if (attempts < maxAttempts) {
                    logger.warn("Error writing ExecutionResult: " + e.toString());
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException interruption) {
                        // Ignore interruption
                        Thread.currentThread().interrupt();
                    }
                } else {
                    throw new ToolException("Error writing ExecutionResult", e);
                }
            }
        }
    }

    private Date now() {
        return Date.from(Instant.now());
    }

    private String getDateTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    private Thread startMonitorThread() {
        thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(monitorThreadPeriod);
                } catch (InterruptedException e) {
                    return;
                }

                try {
                    updateStatusDate();
                } catch (ToolException e) {
                    logger.error("Error updating status date", e);
                }
            }
        });
        thread.start();
        return thread;
    }
}

