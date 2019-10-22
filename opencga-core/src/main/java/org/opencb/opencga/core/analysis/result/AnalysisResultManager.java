package org.opencb.opencga.core.analysis.result;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exception.AnalysisException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.function.Consumer;

public class AnalysisResultManager {

    public static final String FILE_NAME = "status.json";
    public static final String RUNNING = "RUNNING";
    public static final String DONE = "DONE";
    public static final String ERROR = "ERROR";
    private final Path outDir;
    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;

    private File file;
    private boolean initialized;
    private boolean closed;

    public AnalysisResultManager(Path outDir) {
        this.outDir = outDir.toAbsolutePath();
        ObjectMapper objectMapper = new ObjectMapper();
        objectWriter = objectMapper.writerFor(AnalysisResult.class).withDefaultPrettyPrinter();
        objectReader = objectMapper.readerFor(AnalysisResult.class);
        initialized = false;
        closed = false;
    }

    public synchronized AnalysisResultManager init(String analysisId, ObjectMap executorParams) throws AnalysisException {
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

        file = outDir.resolve(FILE_NAME).toFile();
        Date now = now();
        AnalysisResult analysisResult = new AnalysisResult()
                .setId(analysisId)
                .setStart(now)
                .setExecutorParams(executorParams);
        analysisResult.getStatus()
                .setDate(now)
                .setId(RUNNING);

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
        if (exception == null) {
            analysisResult.getStatus()
                    .setCompletedPercentage(100)
                    .setStep("")
                    .setId(DONE);
        } else {
            analysisResult.getStatus()
                    .setId(ERROR);
        }

        write(analysisResult);
        return analysisResult;
    }

    public void addWarning(String warningMessage) throws AnalysisException {
        updateResult(analysisResult -> analysisResult.getWarnings().add(warningMessage));
    }

    public void addFile(Path file, FileResult.FileType fileType) throws AnalysisException {
        String fileStr = file.toAbsolutePath().toString();
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

    public void startStep(String stepId) throws AnalysisException {
        startStep(stepId, null);
    }

    public void startStep(String stepId, Float newTotalPercentage) throws AnalysisException {
        updateResult(analysisResult -> {
            if (newTotalPercentage != null) {
                analysisResult.getStatus().setCompletedPercentage(newTotalPercentage);
            }
            analysisResult.getStatus().setStep(stepId);
            analysisResult
                    .getSteps()
                    .add(new AnalysisStep()
                            .setId(stepId)
                            .setStart(now())
                            .setStatus(new Status()
                                    .setId(RUNNING)
                                    .setDate(now())
                            )
                    );
        });
    }

    public void endStep(float newTotalPercentage) throws AnalysisException {
        updateResult(analysisResult -> {
            AnalysisStep step = analysisResult.getSteps().get(analysisResult.getSteps().size() - 1);
            step.setEnd(now());
            step.getStatus().setId(DONE).setCompletedPercentage(100);
            analysisResult.getStatus()
                    .setStep("")
                    .setCompletedPercentage(newTotalPercentage);
        });
    }

    public void updateResult(Consumer<AnalysisResult> update) throws AnalysisException {
        AnalysisResult analysisResult = read();
        update.accept(analysisResult);
        write(analysisResult);
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

