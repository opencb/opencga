package org.opencb.opencga.analysis;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.OpenCGAResult;

public class AnalysisUtils {

    public static File getBamFileBySampleId(String sampleId, String studyId, FileManager fileManager, String token) throws ToolException {
        // Look for the bam file for each sample
        OpenCGAResult<File> fileQueryResult;

        Query query = new Query(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM)
                .append(FileDBAdaptor.QueryParams.SAMPLES.key(), sampleId);
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

        query.put(FileDBAdaptor.QueryParams.SAMPLES.key(), sampleId);
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

    public static File getCatalogFile(String baitFile, String studyId, FileManager fileManager, String token) {
        OpenCGAResult<File> fileQueryResult;

        Query query = new Query(FileDBAdaptor.QueryParams.ID.key(), baitFile);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UUID.key());

        try {
            fileQueryResult = fileManager.search(studyId, query, queryOptions, token);
        } catch (CatalogException e) {
            return null;
        }

        // Sanity check
        if (fileQueryResult.getNumResults() != 1) {
            return null;
        }

        return fileQueryResult.first();
    }
}
