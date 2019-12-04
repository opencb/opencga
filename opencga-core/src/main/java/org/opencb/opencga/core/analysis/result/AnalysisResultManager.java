package org.opencb.opencga.core.analysis.result;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.analysis.OpenCgaAnalysisExecutor;
import org.opencb.opencga.core.exception.AnalysisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.AccessDeniedException;
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

public class AnalysisResultManager {
    public static final String FILE_EXTENSION = ".result.json";
    public static final String SWAP_FILE_EXTENSION = ".swap" + FILE_EXTENSION;

    private final String analysisId;
    private final Path outDir;
    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;

    private Thread thread;
    private File file;
    private File swapFile;
    private boolean initialized;
    private boolean closed;
    private final Logger logger = LoggerFactory.getLogger(AnalysisResultManager.class);
    private int monitorThreadPeriod = 60000;

    public AnalysisResultManager(String analysisId, Path outDir) throws AnalysisException {
        this.analysisId = analysisId;
        this.outDir = outDir.toAbsolutePath();
        ObjectMapper objectMapper = new ObjectMapper();
//        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectWriter = objectMapper.writerFor(AnalysisResult.class).withDefaultPrettyPrinter();
        objectReader = objectMapper.readerFor(AnalysisResult.class);
        initialized = false;
        closed = false;

        file = outDir.toFile();

        if (!file.exists()) {
            throw new AnalysisException("Output directory '" + outDir + "' does not exist");
        }
        if (!file.isDirectory()) {
            throw new AnalysisException("Output directory '" + outDir + "' does not a directory");
        }
        if (!file.canWrite()) {
            throw new AnalysisException("Write permission denied for output directory '" + outDir + "'");
        }
        if (!StringUtils.isAlphanumeric(analysisId.replaceAll("[-_]", ""))) {
            throw new AnalysisException("Invalid AnalysisID. The analysis id can only contain alphanumeric characters, ',' and '_'.");
        }

        file = outDir.resolve(analysisId + FILE_EXTENSION).toFile();
        swapFile = outDir.resolve(analysisId + SWAP_FILE_EXTENSION).toFile();
    }

    public synchronized AnalysisResultManager init(ObjectMap params, ObjectMap executorParams) throws AnalysisException {
        if (initialized) {
            throw new AnalysisException("AnalysisResultManager already initialized!");
        }
        initialized = true;

        Date now = now();
        AnalysisResult analysisResult = new AnalysisResult()
                .setId(analysisId)
                .setParams(params)
                .setExecutor(new ExecutorInfo()
                        .setId(executorParams.getString(OpenCgaAnalysisExecutor.EXECUTOR_ID))
                        .setParams(executorParams))
                .setStart(now);
        analysisResult.getStatus()
                .setDate(now)
                .setName(Status.Type.RUNNING);

        write(analysisResult);
        startMonitorThread();
        return this;
    }

    public AnalysisResultManager setMonitorThreadPeriod(int monitorThreadPeriod) {
        this.monitorThreadPeriod = monitorThreadPeriod;
        return this;
    }

    public void setSteps(List<String> steps) throws AnalysisException {
        updateResult(analysisResult -> {
            analysisResult.setSteps(new ArrayList<>(steps.size()));
            for (String step : steps) {
                analysisResult.getSteps().add(new AnalysisStep(step, null, null, Status.Type.PENDING, new ObjectMap()));
            }
            return null;
        });
    }

    public boolean isClosed() {
        return closed;
    }

    public synchronized AnalysisResult close() throws AnalysisException {
        return close(null);
    }

    public synchronized AnalysisResult close(Exception exception) throws AnalysisException {
        if (closed) {
            throw new AnalysisException("AnalysisResultManager already closed!");
        }
        thread.interrupt();

        AnalysisResult analysisResult = read();

        Date now = now();
        analysisResult.setEnd(now);
        analysisResult.getStatus()
                .setDate(now);

        AnalysisStep step;
        if (StringUtils.isEmpty(analysisResult.getStatus().getStep())) {
            if (CollectionUtils.isEmpty(analysisResult.getSteps())) {
                analysisResult.setSteps(Collections.singletonList(new AnalysisStep().setId("check")));
            }
            step = analysisResult.getSteps().get(0);
            step.setStart(analysisResult.getStart());
        } else {
            step = getStep(analysisResult, analysisResult.getStatus().getStep());
        }

        Status.Type finalStatus;
        if (exception == null) {
            finalStatus = Status.Type.DONE;
            for (Event event : analysisResult.getEvents()) {
                if (event.getType().equals(Event.Type.ERROR)) {
                    // If there is any ERROR event the final status will be ERROR
                    finalStatus = Status.Type.ERROR;
                    break;
                }
            }
        } else {
            addError(exception, analysisResult);
            finalStatus = Status.Type.ERROR;
        }

        analysisResult.getStatus()
                .setStep(null)
                .setName(finalStatus);

        if (Status.Type.RUNNING.equals(step.getStatus()) || Status.Type.PENDING.equals(step.getStatus())) {
            step.setStatus(finalStatus);
            step.setEnd(now);
        }

        write(analysisResult);
        closed = true;
        return analysisResult;
    }

    public void setExecutorInfo(ExecutorInfo executorInfo) throws AnalysisException {
        updateResult(analysisResult -> analysisResult.setExecutor(executorInfo));
    }

    public void addWarning(String warningMessage) throws AnalysisException {
        updateResult(analysisResult -> analysisResult.getEvents().add(new Event(Event.Type.WARNING, warningMessage)));
    }

    public void addError(Exception exception) throws AnalysisException {
        updateResult(analysisResult -> addError(exception, analysisResult));
    }

    private boolean addError(Exception exception, AnalysisResult analysisResult) {
        return analysisResult.getEvents().add(new Event(Event.Type.ERROR, exception.getMessage()));
    }

    public void addAttribute(String key, Object value) throws AnalysisException {
        updateResult(analysisResult -> analysisResult.getAttributes().put(key, value));
    }

    public void addStepAttribute(String key, Object value) throws AnalysisException {
        updateResult(analysisResult -> {
            AnalysisStep step;
            if (StringUtils.isEmpty(analysisResult.getStatus().getStep())) {
                step = analysisResult.getSteps().get(0);
            } else {
                step = getStep(analysisResult, analysisResult.getStatus().getStep());
            }
            return step.getAttributes().put(key, value);
        });
    }

    public void addFile(Path file, FileResult.FileType fileType) throws AnalysisException {
        String fileStr = file.toAbsolutePath().toString();
        if (!file.toFile().exists()) {
            throw new AnalysisException("No such file or directory: " + fileStr);
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
        updateResult(analysisResult -> analysisResult.getOutputFiles().add(new FileResult(finalFileStr, fileType)));
    }

    public void errorStep() throws AnalysisException {
        updateResult(analysisResult -> getStep(analysisResult, analysisResult.getStatus().getStep())
                .setStatus(Status.Type.ERROR).setEnd(now()));
    }

    public boolean checkStep(String stepId) throws AnalysisException {
        return updateResult(analysisResult -> {

            if (StringUtils.isNotEmpty(analysisResult.getStatus().getStep())) {
                // End previous step

                AnalysisStep step = getStep(analysisResult, analysisResult.getStatus().getStep());
                if (step.getStatus().equals(Status.Type.RUNNING)) {
                    step.setStatus(Status.Type.DONE);
                    step.setEnd(now());
                }
            }

            analysisResult.getStatus().setStep(stepId);
            AnalysisStep step = getStep(analysisResult, stepId);
            if (step.getStatus().equals(Status.Type.DONE)) {
                return false;
            } else {
                step.setStatus(Status.Type.RUNNING);
                step.setStart(now());
                return true;
            }
        });
    }

    private AnalysisStep getStep(AnalysisResult analysisResult, String stepId) throws AnalysisException {
        for (AnalysisStep step : analysisResult.getSteps()) {
            if (step.getId().equals(stepId)) {
                return step;
            }
        }

        List<String> steps = analysisResult.getSteps().stream().map(AnalysisStep::getId).collect(Collectors.toList());

        throw new AnalysisException("Step '" + stepId + "' not found. Available steps: " + steps);
    }

    public void setParams(ObjectMap params) throws AnalysisException {
        updateResult(analysisResult -> analysisResult.setParams(params));
    }

    private void updateStatusDate() throws AnalysisException {
        updateResult(analysisResult -> analysisResult.getStatus().setDate(now()));
    }

    @FunctionalInterface
    public interface AnalysisResultFunction<R> {
        R apply(AnalysisResult analysisResult) throws AnalysisException;
    }

    private synchronized <R> R updateResult(AnalysisResultFunction<R> update) throws AnalysisException {
        AnalysisResult analysisResult = read();
        R apply = update.apply(analysisResult);
        write(analysisResult);
        return apply;
    }

    public AnalysisResult read() throws AnalysisException {
        try {
            return objectReader.readValue(file);
        } catch (IOException e) {
            throw new AnalysisException("Error reading AnalysisResult", e);
        }

    }

    private synchronized void write(AnalysisResult analysisResult) throws AnalysisException {
        int maxAttempts = 3;
        int attempts = 0;
        while (attempts < maxAttempts) {
            attempts++;
            try {
                if (attempts < maxAttempts) {
                    // Perform atomic writes using an intermediate temporary swap file
                    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(swapFile))) {
                        objectWriter.writeValue(os, analysisResult);
                    }
                    Files.move(swapFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } else {
                    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
                        objectWriter.writeValue(os, analysisResult);
                    }
                }
            } catch (IOException e) {
                if (attempts < maxAttempts) {
                    logger.warn("Error writing AnalysisResult: " + e.toString());
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException interruption) {
                        // Ignore interruption
                        Thread.currentThread().interrupt();
                    }
                } else {
                    throw new AnalysisException("Error writing AnalysisResult", e);
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
                } catch (AnalysisException e) {
                    logger.error("Error updating status date", e);
                }
            }
        });
        thread.start();
        return thread;
    }
}

