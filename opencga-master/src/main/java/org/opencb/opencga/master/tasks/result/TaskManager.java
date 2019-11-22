package org.opencb.opencga.master.tasks.result;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.core.analysis.result.Status;
import org.opencb.opencga.master.exceptions.TaskException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class TaskManager {

    public static final String FILE_EXTENSION = ".result.json";

    private final String taskId;
    private final Path outDir;
    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;

    private Thread thread;
    private File file;
    private boolean initialized;
    private boolean closed;
    private final Logger logger = LoggerFactory.getLogger(TaskManager.class);
    private int monitorThreadPeriod = 5000;

    public TaskManager(String taskId, Path outDir) throws TaskException {
        this.taskId = taskId;
        this.outDir = outDir.toAbsolutePath();
        ObjectMapper objectMapper = new ObjectMapper();
        objectWriter = objectMapper.writerFor(Result.class).withDefaultPrettyPrinter();
        objectReader = objectMapper.readerFor(Result.class);
        initialized = false;
        closed = false;

        file = outDir.toFile();

        if (!file.exists()) {
            throw new TaskException("Output directory '" + outDir + "' does not exist");
        }
        if (!file.isDirectory()) {
            throw new TaskException("Output directory '" + outDir + "' is not actually a directory");
        }
        if (!file.canWrite()) {
            throw new TaskException("Write permission denied for output directory '" + outDir + "'");
        }
        if (!StringUtils.isAlphanumeric(taskId.replaceAll("[-_]", ""))) {
            throw new TaskException("Invalid task id. Task id can only contain alphanumeric characters, ',' and '_'.");
        }

        file = outDir.resolve(taskId + FILE_EXTENSION).toFile();
    }

    public synchronized TaskManager init() throws TaskException {
        if (initialized) {
            throw new TaskException("TaskManager already initialized!");
        }
        initialized = true;

        Date now = now();
        Result taskResult = new Result()
                .setStart(now);
        taskResult.getStatus()
                .setDate(now)
                .setName(Status.Type.RUNNING);

        write(taskResult);
        startMonitorThread();
        return this;
    }

    public TaskManager setMonitorThreadPeriod(int monitorThreadPeriod) {
        this.monitorThreadPeriod = monitorThreadPeriod;
        return this;
    }

    public void setSteps(List<String> steps) throws TaskException {
        updateResult(taskResult -> {
            taskResult.setSteps(new ArrayList<>(steps.size()));
            for (String step : steps) {
                taskResult.getSteps().add(new Step(step, null, null, Status.Type.PENDING));
            }
            return null;
        });
    }

    public boolean isClosed() {
        return closed;
    }

    public synchronized Result close() throws TaskException {
        return close(null);
    }

    public synchronized Result close(Exception exception) throws TaskException {
        if (closed) {
            throw new TaskException("TaskManager already closed!");
        }
        thread.interrupt();

        Result taskResult = read();

        Date now = now();
        taskResult.setEnd(now);
        taskResult.getStatus()
                .setDate(now);

        Step step;
        if (StringUtils.isEmpty(taskResult.getStatus().getStep())) {
            if (CollectionUtils.isEmpty(taskResult.getSteps())) {
                taskResult.setSteps(Collections.singletonList(new Step().setId("check")));
            }
            step = taskResult.getSteps().get(0);
            step.setStart(taskResult.getStart());
        } else {
            step = getStep(taskResult, taskResult.getStatus().getStep());
        }

        Status.Type finalStatus;
        if (exception == null) {
            finalStatus = Status.Type.DONE;
        } else {
            addError(exception, taskResult);
            finalStatus = Status.Type.ERROR;
        }

        taskResult.getStatus()
                .setStep(null)
                .setName(finalStatus);

        if (Status.Type.RUNNING.equals(step.getStatus()) || Status.Type.PENDING.equals(step.getStatus())) {
            step.setStatus(finalStatus);
            step.setEnd(now);
        }

        write(taskResult);
        closed = true;
        return taskResult;
    }

    public void addWarning(String warningMessage) throws TaskException {
        updateResult(taskResult -> taskResult.getEvents().add(new Event(Event.Type.WARNING, warningMessage)));
    }

    public void addError(Exception exception) throws TaskException {
        updateResult(taskResult -> addError(exception, taskResult));
    }

    private boolean addError(Exception exception, Result taskResult) {
        return taskResult.getEvents().add(new Event(Event.Type.ERROR, exception.getMessage()));
    }

    public void errorStep() throws TaskException {
        updateResult(taskResult -> getStep(taskResult, taskResult.getStatus().getStep())
                .setStatus(Status.Type.ERROR).setEnd(now()));
    }

    public boolean checkStep(String stepId) throws TaskException {
        return updateResult(taskResult -> {

            if (StringUtils.isNotEmpty(taskResult.getStatus().getStep())) {
                // End previous step

                Step step = getStep(taskResult, taskResult.getStatus().getStep());
                if (step.getStatus().equals(Status.Type.RUNNING)) {
                    step.setStatus(Status.Type.DONE);
                    step.setEnd(now());
                }
            }

            taskResult.getStatus().setStep(stepId);
            Step step = getStep(taskResult, stepId);
            if (step.getStatus().equals(Status.Type.DONE)) {
                return false;
            } else {
                step.setStatus(Status.Type.RUNNING);
                step.setStart(now());
                return true;
            }
        });
    }

    private Step getStep(Result taskResult, String stepId) throws TaskException {
        for (Step step : taskResult.getSteps()) {
            if (step.getId().equals(stepId)) {
                return step;
            }
        }

        List<String> steps = taskResult.getSteps().stream().map(Step::getId).collect(Collectors.toList());

        throw new TaskException("Step '" + stepId + "' not found. Available steps: " + steps);
    }

    private void updateStatusDate() throws TaskException {
        updateResult(taskResult -> taskResult.getStatus().setDate(now()));
    }

    @FunctionalInterface
    public interface ResultFunction<R> {
        R apply(Result taskResult) throws TaskException;
    }

    private synchronized <R> R updateResult(TaskManager.ResultFunction<R> update) throws TaskException {
        Result taskResult = read();
        R apply = update.apply(taskResult);
        write(taskResult);
        return apply;
    }

    public Result read() throws TaskException {
        try {
            return objectReader.readValue(file);
        } catch (IOException e) {
            throw new TaskException("Error reading Result", e);
        }

    }

    private void write(Result taskResult) throws TaskException {
        try {
            objectWriter.writeValue(file, taskResult);
        } catch (IOException e) {
            throw new TaskException("Error writing Result", e);
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
                } catch (TaskException e) {
                    logger.error("Error updating status date", e);
                }
            }
        });
        thread.start();
        return thread;
    }


}
