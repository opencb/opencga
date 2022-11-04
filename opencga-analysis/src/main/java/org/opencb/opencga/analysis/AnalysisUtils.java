package org.opencb.opencga.analysis;

import org.opencb.biodata.models.clinical.qc.RelatednessScore;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.JobManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

import static org.opencb.opencga.core.api.ParamConstants.SAMTOOLS_COMMANDS_SUPPORTED;

public class AnalysisUtils {

    public static boolean isSupportedCommand(String commands) {
        Set<String> commandSet = new HashSet<>(Arrays.asList(commands.replace(" ", "").split(",")));
        if (!commandSet.contains(commands)) {
            return true;
        }
        return false;
    }

    public static File getBamFileBySampleId(String sampleId, String studyId, FileManager fileManager, String token) throws ToolException {
        // Look for the bam file for each sample
        OpenCGAResult<File> fileQueryResult;

        Query query = new Query(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM)
                .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleId);
        try {
            fileQueryResult = fileManager.search(studyId, query, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Sanity check
        if (fileQueryResult.getNumResults() > 1) {
            throw new ToolException("Found more than one BAM files (" + fileQueryResult.getNumResults() + ") for sample " + sampleId);
        }

        return (fileQueryResult.getNumResults() == 0) ? null : fileQueryResult.first();
    }

    public static File getBamFile(String filename, String sampleId, String studyId, FileManager fileManager, String token) throws ToolException {
        // Look for the bam file for each sample
        OpenCGAResult<File> fileQueryResult;

        Query query = new Query(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM);
        QueryOptions queryOptions = new QueryOptions();//QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UUID.key());

        query.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleId);
        try {
            fileQueryResult = fileManager.search(studyId, query, queryOptions, token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Sanity check
        if (fileQueryResult.getNumResults() == 0) {
            throw new ToolException("No BAM files found for sample " + sampleId);
        }
        for (File file : fileQueryResult.getResults()) {
            System.out.println("===> filename = " + filename + " -> comparing to " + file.getId() + ", " + file.getPath() + ", " + file.getName()
                    + ", " + file.getUuid() + ", " + file.getPath());
            if (filename.equals(file.getId()) || filename.equals(file.getPath()) || filename.equals(file.getName())
                    || filename.equals(file.getUuid())) {
                return file;
            }
        }

        throw new ToolException("BAM file " + filename + " not found for sample " + sampleId);
    }

    public static File getCatalogFile(String file, String studyId, FileManager fileManager, String token) throws CatalogException {
        OpenCGAResult<File> fileQueryResult;

        Query query = new Query(FileDBAdaptor.QueryParams.ID.key(), file);

        fileQueryResult = fileManager.search(studyId, query, QueryOptions.empty(), token);

        // Sanity check
        if (fileQueryResult.getNumResults() == 0) {
            throw new CatalogException("File '" + file + "' not found in study '" + studyId  + "'");
        }

        if (fileQueryResult.getNumResults() > 1) {
            throw new CatalogException("More than one file '" + file + "' found (" + fileQueryResult.getNumResults() + ") in study '"
                    + studyId  + "'");
        }

        return fileQueryResult.first();
    }

    public static Map<String, Map<String, Float>> parseRelatednessThresholds(Path thresholdsPath) throws IOException {
        Map<String, Map<String, Float>> thresholds = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(thresholdsPath.toFile()));
        String line;
        while ((line = reader.readLine()) != null) {
            if (!line.startsWith("#")) {
                String[] splits = line.trim().split("\t");
                if (splits.length != 9) {
                    new IOException("Error parsing relatedness thresholds file: " + thresholdsPath.toFile().getName());
                }
                Map<String, Float> scores = new HashMap<>();
                scores.put("minPiHat", Float.parseFloat(splits[1]));
                scores.put("maxPiHat", Float.parseFloat(splits[2]));
                scores.put("minZ0", Float.parseFloat(splits[3]));
                scores.put("maxZ0", Float.parseFloat(splits[4]));
                scores.put("minZ1", Float.parseFloat(splits[5]));
                scores.put("maxZ1", Float.parseFloat(splits[6]));
                scores.put("minZ2", Float.parseFloat(splits[7]));
                scores.put("maxZ2", Float.parseFloat(splits[8]));

                thresholds.put(splits[0], scores);
            }
        }
        return thresholds;
    }

    public static boolean waitFor(String jobId, String study, JobManager jobManager, String token) throws ToolException, CatalogException {
        Query query = new Query("id", jobId);
        OpenCGAResult<Job> result = jobManager.search(study, query, QueryOptions.empty(), token);
        Job job = result.first();
        String status = job.getInternal().getStatus().getId();

        while (status.equals(Enums.ExecutionStatus.PENDING) || status.equals(Enums.ExecutionStatus.RUNNING)
                || status.equals(Enums.ExecutionStatus.QUEUED) || status.equals(Enums.ExecutionStatus.READY)
                || status.equals(Enums.ExecutionStatus.REGISTERING)) {
            try {
                // Sleep for 30 seconds
                Thread.sleep(30000);
                result = jobManager.search(study, query, QueryOptions.empty(), token);
                job = result.first();
            } catch (CatalogException | InterruptedException e) {
                new ToolException("Error waiting for job '" + jobId + "': " + e.getMessage());
            }
            status = job.getInternal().getStatus().getId();
        }
        return status.equals(Enums.ExecutionStatus.DONE) ? true : false;
    }

    public static Job getJob(String jobId, String study, JobManager jobManager, String token) throws ToolException, CatalogException {
        Query query = new Query("id", jobId);
        OpenCGAResult<Job> result = jobManager.search(study, query, QueryOptions.empty(), token);
        Job job = result.first();
        if (job == null) {
            new ToolException("Error getting job '" + jobId + "' from study '" + study + "'.");
        }
        return job;
    }
}
