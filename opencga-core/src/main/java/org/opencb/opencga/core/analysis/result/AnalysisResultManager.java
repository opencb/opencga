package org.opencb.opencga.core.analysis.result;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.analysis.OpenCgaAnalysisExecutor;
import org.opencb.opencga.core.exception.AnalysisException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class AnalysisResultManager {
    public static final String FILE_EXTENSION = ".result.json";

    private final String analysisId;
    private final Path outDir;
    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;

    private File file;
    private boolean initialized;
    private boolean closed;

    public AnalysisResultManager(String analysisId, Path outDir) {
        this.analysisId = analysisId;
        this.outDir = outDir.toAbsolutePath();
        ObjectMapper objectMapper = new ObjectMapper();
//        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectWriter = objectMapper.writerFor(AnalysisResult.class).withDefaultPrettyPrinter();
        objectReader = objectMapper.readerFor(AnalysisResult.class);
        initialized = false;
        closed = false;
    }

    public synchronized AnalysisResultManager init(ObjectMap executorParams, List<String> steps) throws AnalysisException {
        if (initialized) {
            throw new AnalysisException("AnalysisResultManager already initialized!");
        }
        initialized = true;
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
        Date now = now();
        AnalysisResult analysisResult = new AnalysisResult()
                .setId(analysisId)
                .setExecutor(new ExecutorInfo()
                        .setId(executorParams.getString(OpenCgaAnalysisExecutor.EXECUTOR_ID))
                        .setParams(executorParams))
                .setStart(now);
        analysisResult.getStatus()
                .setDate(now)
                .setId(Status.RUNNING);

        analysisResult.setSteps(new ArrayList<>(steps.size()));
        for (String step : steps) {
            analysisResult.getSteps().add(new AnalysisStep(step, null, null, Status.NONE, new ObjectMap()));
        }

        write(analysisResult);
        return this;
    }

    public synchronized AnalysisResult close() throws AnalysisException {
        return close(null);
    }

    public AnalysisResult close(Exception exception) throws AnalysisException {
        if (closed) {
            throw new AnalysisException("AnalysisResultManager already closed!");
        }
        closed = true;

        AnalysisResult analysisResult = read();

        Date now = now();
        analysisResult.setEnd(now);
        analysisResult.getStatus()
                .setDate(now);

        AnalysisStep step;
        if (StringUtils.isEmpty(analysisResult.getStatus().getStep())) {
            step = analysisResult.getSteps().get(0);
            step.setStart(analysisResult.getStart());
        } else {
            step = getStep(analysisResult, analysisResult.getStatus().getStep());
        }

        String finalStatus;
        if (exception == null) {
            finalStatus = Status.DONE;
        } else {
            analysisResult.getWarnings().add(exception.getMessage());
            finalStatus = Status.ERROR;
        }

        analysisResult.getStatus()
                .setStep(null)
                .setId(finalStatus);

        if (step.getStatus().equals(Status.RUNNING)) {
            step.setStatus(finalStatus);
            step.setEnd(now);
        }

        write(analysisResult);
        return analysisResult;
    }

    public void setExecutorInfo(ExecutorInfo executorInfo) throws AnalysisException {
        updateResult(analysisResult -> analysisResult.setExecutor(executorInfo));
    }

    public void addWarning(String warningMessage) throws AnalysisException {
        updateResult(analysisResult -> analysisResult.getWarnings().add(warningMessage));
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

    public void skipStep(String stepId) throws AnalysisException {
        updateResult(analysisResult -> getStep(analysisResult, stepId).setStatus(Status.SKIP).setEnd(now()));
    }

    public void skipStep() throws AnalysisException {
        updateResult(analysisResult -> getStep(analysisResult, analysisResult.getStatus().getStep()).setStatus(Status.SKIP).setEnd(now()));
    }

    public void errorStep(String stepId) throws AnalysisException {
        updateResult(analysisResult -> getStep(analysisResult, stepId).setStatus(Status.ERROR).setEnd(now()));
    }

    public boolean checkStep(String stepId) throws AnalysisException {
        return updateResult(analysisResult -> {

            if (StringUtils.isNotEmpty(analysisResult.getStatus().getStep())) {
                // End previous step

                AnalysisStep step = getStep(analysisResult, analysisResult.getStatus().getStep());
                if (step.getStatus().equals(Status.RUNNING)) {
                    step.setStatus(Status.DONE);
                    step.setEnd(now());
                }
            }

            analysisResult.getStatus().setStep(stepId);
            AnalysisStep step = getStep(analysisResult, stepId);
            if (step.getStatus().equals(Status.DONE)) {
                return false;
            } else {
                step.setStatus(Status.RUNNING);
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

    @FunctionalInterface
    public interface AnalysisResultFunction<R> {
        R apply(AnalysisResult analysisResult) throws AnalysisException;
    }

    private <R> R updateResult(AnalysisResultFunction<R> update) throws AnalysisException {
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

    private void write(AnalysisResult analysisResult) throws AnalysisException {
        try {
            objectWriter.writeValue(file, analysisResult);
        } catch (IOException e) {
            throw new AnalysisException("Error writing AnalysisResult", e);
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

}

