package org.opencb.opencga.storage.core.variant.io;

import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.core.common.ProgressLogger;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.metadata.ExportMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPOutputStream;

/**
 * Prints the result of a given query in the selected output format, and the associated metadata.
 *
 * This class is intended to be extended by other exporters.
 *
 * Created on 06/12/16.
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantExporter {

    public static final String METADATA_FILE_EXTENSION = ".meta.json.gz";
    private final VariantDBAdaptor dbAdaptor;
    private final VariantWriterFactory variantWriterFactory;

    public VariantExporter(VariantDBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
        variantWriterFactory = new VariantWriterFactory(dbAdaptor);
    }

    /**
     * Exports the result of the given query and the associated metadata.
     * @param outputFileUri Optional output file. If null or empty, will print into the Standard output. Won't export any metadata.
     * @param outputFormat  Output format.
     * @param query         Query with the variants to export
     * @param queryOptions  Query options
     * @throws IOException  If there is any IO error
     * @throws StorageManagerException  If there is any error exporting variants
     */
    public void export(@Nullable URI outputFileUri, String outputFormat, Query query, QueryOptions queryOptions)
            throws IOException, StorageManagerException {

        String outputFile = null;
        if (outputFileUri != null) {
            outputFile = outputFileUri.getPath();
        }
        outputFile = VariantWriterFactory.checkOutput(outputFile, outputFormat);
        List<Integer> studyIds = dbAdaptor.getReturnedStudies(query, QueryOptions.empty());

        try (OutputStream os = VariantWriterFactory.getOutputStream(outputFile, outputFormat)) {
            boolean logProgress = !VariantWriterFactory.isStandardOutput(outputFile);
            exportData(os, outputFormat, query, queryOptions, logProgress);
        }
        if (!VariantWriterFactory.isStandardOutput(outputFile)) {
            exportMetaData(query, studyIds, outputFile + METADATA_FILE_EXTENSION);
        }
    }

    protected void exportData(OutputStream outputStream, String outputFormat, Query query, QueryOptions queryOptions,
                              boolean logProgress)
            throws StorageManagerException, IOException {
        if (query == null) {
            query = new Query();
        }
        if (queryOptions == null) {
            queryOptions = new QueryOptions();
        }

        // DataReader
        VariantDBReader variantDBReader = new VariantDBReader(dbAdaptor, query, queryOptions);

        // Task<Variant, Variant>
        ParallelTaskRunner.TaskWithException<Variant, Variant, Exception> progressTask;
        if (logProgress) {
            progressTask = batch -> batch;
        } else {
            final Query finalQuery = query;
            final QueryOptions finalQueryOptions = queryOptions;
            ProgressLogger progressLogger = new ProgressLogger("Export variants", () -> {
                Long count = dbAdaptor.count(finalQuery).first();
                long limit = finalQueryOptions.getLong(QueryOptions.LIMIT, Long.MAX_VALUE);
                long skip = finalQueryOptions.getLong(QueryOptions.SKIP, 0);
                count = Math.min(limit, count - skip);
                return count;
            }, 200);
            progressTask = batch -> {
                progressLogger.increment(batch.size(), () -> "up to position " + batch.get(batch.size() - 1).toString());
                return batch;
            };
        }

        // DataWriter
        DataWriter<Variant> variantDataWriter = variantWriterFactory.newDataWriter(outputFormat, outputStream, query, queryOptions);

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().setNumTasks(1).setBatchSize(10).build();

        ParallelTaskRunner<Variant, Variant> ptr = new ParallelTaskRunner<>(variantDBReader, progressTask, variantDataWriter, config);
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageManagerException("Error exporting variants", e);
        }
    }

    protected void exportMetaData(Query query, List studies, String output) throws IOException {
        StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();

        List<Integer> studyIds = dbAdaptor.getDBAdaptorUtils().getStudyIds(studies, QueryOptions.empty());
        List<StudyConfiguration> studyConfigurations = new ArrayList<>(studyIds.size());
        for (Integer studyId : studyIds) {
            studyConfigurations.add(scm.getStudyConfiguration(studyId, QueryOptions.empty()).first());
        }

        ExportMetadata exportMetadata = new ExportMetadata(studyConfigurations, query);

        writeMetadata(exportMetadata, output);

    }

    protected void writeMetadata(ExportMetadata exportMetadata, String output) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        File file = Paths.get(output).toFile();
        try (OutputStream os = new GZIPOutputStream(new FileOutputStream(file))) {
            objectMapper.writeValue(os, exportMetadata);
        }
    }

}
