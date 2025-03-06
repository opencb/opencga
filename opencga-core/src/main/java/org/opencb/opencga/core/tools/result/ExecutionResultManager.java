/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.tools.result;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.ExceptionUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.tools.ToolDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExecutionResultManager {

    private static final String FILE_EXTENSION = ".result.json";
    private static final String SWAP_FILE_EXTENSION = ".swap" + FILE_EXTENSION;

    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;

    private Thread thread;
    private File file;
    private final File swapFile;
    private final List<File> oldRollingFiles;
    private int rollingFilesCounter;
    private final String rollingFileFormat;
    private boolean initialized;
    private boolean closed;
    private static final Logger logger = LoggerFactory.getLogger(ExecutionResultManager.class);
    private long monitorThreadPeriod = TimeUnit.MINUTES.toMillis(1);

    public ExecutionResultManager(String toolId, Path outDir) throws ToolException {
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
        if (!StringUtils.isAlphanumeric(toolId.replaceAll("[-_.]", ""))) {
            throw new ToolException("Invalid ToolId. The tool id can only contain alphanumeric characters, '.', '-' and '_'");
        }

        file = outDir.resolve(toolId + FILE_EXTENSION).toFile();
        rollingFileFormat = outDir.resolve(toolId + ".%d" + FILE_EXTENSION).toAbsolutePath().toString();
        oldRollingFiles = new LinkedList<>();
        rollingFilesCounter = 0;
        swapFile = outDir.resolve(toolId + SWAP_FILE_EXTENSION).toFile();
    }

    public synchronized ExecutionResultManager init(ObjectMap params, ObjectMap executorParams) throws ToolException {
        return init(params, executorParams, true);
    }

    public synchronized ExecutionResultManager init(ObjectMap params, ObjectMap executorParams, boolean startMonitor) throws ToolException {
        if (initialized) {
            throw new ToolException(getClass().getName() + " already initialized!");
        }
        initialized = true;
        Date now = now();
        ExecutionResult execution = new ExecutionResult()
                .setExecutor(new ExecutorInfo()
                        .setId(executorParams.getString(OpenCgaToolExecutor.EXECUTOR_ID))
                        .setParams(removeTokenFromParams(executorParams)))
                .setStart(now);
        execution.getStatus()
                .setDate(now)
                .setName(Status.Type.RUNNING);

        write(execution);
        if (startMonitor) {
            startMonitorThread();
        }
        return this;
    }

    public ExecutionResultManager setMonitorThreadPeriod(int monitorThreadPeriod) {
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

    public synchronized ExecutionResult close(Throwable throwable) throws ToolException {
        if (closed) {
            throw new ToolException(getClass().getName() + " already closed!");
        }
        if (thread != null) {
            thread.interrupt();
        }

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
        if (throwable == null) {
            finalStatus = Status.Type.DONE;
            for (Event event : execution.getEvents()) {
                if (event.getType().equals(Event.Type.ERROR)) {
                    // If there is any ERROR event the final status will be ERROR
                    finalStatus = Status.Type.ERROR;
                    break;
                }
            }
            for (ToolStep executionStep : execution.getSteps()) {
                if (Status.Type.ERROR.equals(executionStep.getStatus())) {
                    // If there is any ERROR on any step, the final status will be ERROR
                    finalStatus = Status.Type.ERROR;
                    break;
                }
            }
        } else {
            addError(throwable, execution);
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
        cleanRollingFiles(new ArrayList<>(), 0);
        closed = true;
        return execution;
    }

    public void setExecutorInfo(ExecutorInfo executorInfo) throws ToolException {
        if (executorInfo != null) {
            ObjectMap params = executorInfo.getParams();
            executorInfo.setParams(removeTokenFromParams(params));
        }
        updateResult(result -> result.setExecutor(executorInfo));
    }

    private ObjectMap removeTokenFromParams(ObjectMap params) {
        if (params != null && params.containsKey(ParamConstants.TOKEN)) {
            ObjectMap paramsCopy = new ObjectMap(params);
            paramsCopy.put(ParamConstants.TOKEN, "xxxxxxxxxxxxxx");
            return paramsCopy;
        }
        return params;
    }

    public void addParam(String key, Object value) throws ToolException {
        updateResult(result -> result.getExecutor().getParams().put(key, value));
    }

    public void addEvent(Event.Type type, String message) throws ToolException {
        updateResult(result -> result.getEvents().add(new Event(type, message)));
    }

    public void addWarning(String warningMessage) throws ToolException {
        updateResult(result -> result.getEvents().add(new Event(Event.Type.WARNING, warningMessage)));
    }

    public void addError(Exception exception) throws ToolException {
        updateResult(result -> addError(exception, result));
    }

    private boolean addError(Throwable exception, ExecutionResult execution) {
        String message = ExceptionUtils.prettyExceptionMessage(exception, false, true);
        return execution.getEvents().add(new Event(
                Event.Type.ERROR,
                exception.getClass().getSimpleName(), message
        ));
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

    public void addDependency(ToolDependency dependency) throws ToolException {
        addDependencies(Collections.singletonList(dependency));
    }

    public void addDependencies(List<ToolDependency> dependencyList) throws ToolException {
        updateResult(result -> {
            result.getDependencies().addAll(dependencyList);
            return null;
        });
    }

    public void setManualSteps(List<ToolStep> steps) throws ToolException {
        updateResult(result -> result.setSteps(steps));
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

    public void addExternalFile(URI file) throws ToolException {
        updateResult(execution -> {
            execution.getExternalFiles().add(file);
            return null;
        });
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
            return read(file);
        } catch (IOException e) {
            if (Files.exists(swapFile.toPath())) {
                try {
                    return read(swapFile);
                } catch (IOException ioException) {
                    e.addSuppressed(ioException);
                }
            }
            throw new ToolException("Error reading ExecutionResult", e);
        }
    }

    private synchronized void write(ExecutionResult execution) throws ToolException {
        int maxAttempts = 3;
        int attempts = 0;
        List<Exception> suppressed = new LinkedList<>();
        while (true) {
            attempts++;
            try {
                if (attempts > 1) {
                    increaseRollingFile();
                    cleanRollingFiles(suppressed, 2);
                }
                // Perform atomic writes using an intermediate temporary swap file
                write(swapFile, execution);
//                logger.info("Moving '{}' -> '{}'", swapFile.toPath(), file.toPath());
                Files.move(swapFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                return;
            } catch (IOException e) {
                if (attempts < maxAttempts) {
                    if (attempts == 1) {
                        // Reduce verbosity on the first failed attempt
                        logger.debug("Error writing ExecutionResult: " + e);
                    } else {
                        logger.warn("Error writing ExecutionResult: " + e);
                    }
                    suppressed.add(e);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException interruption) {
                        // Ignore interruption
                        Thread.currentThread().interrupt();
                        suppressed.add(interruption);
                    }
                } else {
                    ToolException exception = new ToolException("Error writing ExecutionResult", e);
                    suppressed.forEach(exception::addSuppressed);
                    throw exception;
                }
            } catch (Exception e) {
                suppressed.forEach(e::addSuppressed);
                throw e;
            }
        }
    }

    private void cleanRollingFiles(List<Exception> suppressed, int keeptFiles) {
        try {
            while (oldRollingFiles.size() > keeptFiles) {
                File rollingFile = oldRollingFiles.get(0);
                if (rollingFile != this.file && rollingFile.exists()) {
                    Files.delete(rollingFile.toPath());
                }
//                logger.debug("Old file '{}' deleted", rollingFile);
                oldRollingFiles.remove(0);
            }
        } catch (IOException e) {
            suppressed.add(e);
        }
    }

    private void increaseRollingFile() {
        rollingFilesCounter++;
        String fileName = String.format(rollingFileFormat, rollingFilesCounter);
//        logger.info("Increase rolling file " + fileName);
        oldRollingFiles.add(file);
        file = Paths.get(fileName).toFile();
    }

    public static Path getExecutionResultPath(Path dir) throws IOException {
        Path resultJson;
        try (Stream<Path> stream = Files.list(dir)) {
            resultJson = stream
                    .filter(path -> isExecutionResultFile(path.toString()))
                    .filter(path -> !isExecutionResultSwapFile(path.toString()))
                    .max(Comparator.comparingInt(p -> {
                        String s = StringUtils.removeEnd(p.toString(), FILE_EXTENSION);
                        int nextDot = s.lastIndexOf('.');
                        if (nextDot < 0) {
                            return 0;
                        } else {
                            s = s.substring(nextDot + 1);
                            try {
                                return Integer.parseInt(s);
                            } catch (NumberFormatException e) {
                                // assume there was no number
                                return 0;
                            }
                        }
                    }))
                    .orElse(null);
        }
        return resultJson;
    }

    public static boolean isExecutionResultFile(String file) {
        return file.endsWith(FILE_EXTENSION);
    }

    public static boolean isExecutionResultSwapFile(String file) {
        return file.endsWith(SWAP_FILE_EXTENSION);
    }

    public static ExecutionResult findAndRead(Path dir, int expirationTimeInSeconds) {
        Pair<Path, ExecutionResult> pair = findAndReadPair(dir);
        if (pair == null) {
            return null;
        } else {
            ExecutionResult result = pair.getValue();
            Instant lastStatusUpdate = result.getStatus().getDate().toInstant();
            if (lastStatusUpdate.until(Instant.now(), ChronoUnit.SECONDS) > expirationTimeInSeconds) {
                logger.warn("Ignoring file '" + pair.getKey() + "'. The file is more than " + expirationTimeInSeconds + " seconds old");
                return null;
            } else {
                return result;
            }
        }
    }

    public static ExecutionResult findAndRead(Path dir) {
        Pair<Path, ExecutionResult> pair = findAndReadPair(dir);
        if (pair != null) {
            return pair.getValue();
        } else {
            return null;
        }
    }

    private static Pair<Path, ExecutionResult> findAndReadPair(Path dir) {
        int attempts = 0;
        int maxAttempts = 3;
        List<Exception> supressed = new ArrayList<>(0);
        while (attempts < maxAttempts) {
            Path file = null;
            try {
                attempts++;
                file = getExecutionResultPath(dir);
                if (file == null) {
                    return null;
                }
                try (InputStream is = new BufferedInputStream(new FileInputStream(file.toFile()))) {
                    ExecutionResult result = JacksonUtils.getDefaultObjectMapper().readValue(is, ExecutionResult.class);
                    return Pair.of(file, result);
                }
            } catch (IOException e) {
                String errorMessage;
                if (file == null) {
                    // Exception looking for the ExecutionResultPath
                    errorMessage = "Could not find ExecutionResult file at dir: " + dir.toAbsolutePath();
                } else {
                    // Exception reading the ExecutionResult file
                    if (Files.exists(file)) {
                        errorMessage = "Could not read ExecutionResult file, file seems corrupted: '" + file.toAbsolutePath() + "'";
                    } else {
                        errorMessage = "Could not read ExecutionResult file, file not found: '" + file.toAbsolutePath() + "'";
                    }
                }
                if (attempts == maxAttempts) {
                    supressed.forEach(e::addSuppressed);
                    logger.error(errorMessage, e);
                } else {
                    logger.warn(errorMessage + ". Retry " + attempts + "/" + maxAttempts + ". " + e.getMessage());
                    supressed.add(e);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException interruption) {
                        // Ignore interruption
                        Thread.currentThread().interrupt();
                        supressed.add(interruption);
                    }
                }
            }
        }
        return null;
    }

    private ExecutionResult read(File file) throws IOException {
        ExecutionResult executionResult;
        try (FileInputStream fis = new FileInputStream(file)) {
            executionResult = objectReader.readValue(fis);
        }
        return executionResult;
    }

    private void write(File file, ExecutionResult execution) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file); OutputStream os = new BufferedOutputStream(fos)) {
            objectWriter.writeValue(os, execution);
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

