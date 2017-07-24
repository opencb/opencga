/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.variant.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ExportMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
    private final VariantStorageEngine engine;
    private final VariantWriterFactory variantWriterFactory;

    private final Logger logger = LoggerFactory.getLogger(VariantExporter.class);

    public VariantExporter(VariantStorageEngine engine) throws StorageEngineException {
        this.engine = engine;
        variantWriterFactory = new VariantWriterFactory(engine.getDBAdaptor());
    }

    /**
     * Exports the result of the given query and the associated metadata.
     * @param outputFileUri Optional output file. If null or empty, will print into the Standard output. Won't export any metadata.
     * @param outputFormat  Variant Output format.
     * @param query         Query with the variants to export
     * @param queryOptions  Query options
     * @throws IOException  If there is any IO error
     * @throws StorageEngineException  If there is any error exporting variants
     */
    public void export(@Nullable URI outputFileUri, VariantOutputFormat outputFormat, Query query, QueryOptions queryOptions)
            throws IOException, StorageEngineException {

        String outputFile = null;
        if (outputFileUri != null) {
            outputFile = outputFileUri.getPath();
        }
        outputFile = VariantWriterFactory.checkOutput(outputFile, outputFormat);
        List<Integer> studyIds = engine.getDBAdaptor().getReturnedStudies(query, QueryOptions.empty());

        try (OutputStream os = VariantWriterFactory.getOutputStream(outputFile, outputFormat)) {
            boolean logProgress = !VariantWriterFactory.isStandardOutput(outputFile);
            exportData(os, outputFormat, query, queryOptions, logProgress);
        }
        if (!VariantWriterFactory.isStandardOutput(outputFile)) {
            exportMetaData(query, queryOptions, studyIds, outputFile + METADATA_FILE_EXTENSION);
        }
    }

    protected void exportData(OutputStream outputStream, VariantOutputFormat outputFormat, Query query, QueryOptions queryOptions,
                              boolean logProgress)
            throws StorageEngineException, IOException {
        if (query == null) {
            query = new Query();
        }
        if (queryOptions == null) {
            queryOptions = new QueryOptions();
        }

        // DataReader
        VariantDBReader variantDBReader = new VariantDBReader(engine.iterator(query, queryOptions));

        // Task<Variant, Variant>
        ParallelTaskRunner.TaskWithException<Variant, Variant, Exception> progressTask;
        if (logProgress) {
            progressTask = batch -> batch;
        } else {
            final Query finalQuery = query;
            final QueryOptions finalQueryOptions = queryOptions;
            ProgressLogger progressLogger = new ProgressLogger("Export variants", () -> {
                Long count = engine.count(finalQuery).first();
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
            throw new StorageEngineException("Error exporting variants", e);
        }

        logger.info("Time fetching data: " + variantDBReader.getTimeFetching(TimeUnit.MILLISECONDS) / 1000.0 + 's');
        logger.info("Time converting data: " + variantDBReader.getTimeConverting(TimeUnit.MILLISECONDS) / 1000.0 + 's');

    }

    protected void exportMetaData(Query query, QueryOptions queryOptions, List studies, String output)
            throws IOException, StorageEngineException {
        StudyConfigurationManager scm = engine.getStudyConfigurationManager();

        Map<Integer, List<Integer>> returnedSamples = VariantQueryUtils.getReturnedSamples(query, queryOptions, scm);
        List<StudyConfiguration> studyConfigurations = new ArrayList<>(returnedSamples.size());
        returnedSamples.forEach((studyId, samplesList) -> {
            StudyConfiguration sc = scm.getStudyConfiguration(studyId, QueryOptions.empty()).first();
            List<Integer> samplesToRemove = new ArrayList<>();
            for (Integer sampleId : sc.getSampleIds().values()) {
                if (!samplesList.contains(sampleId)) {
                    samplesToRemove.add(sampleId);
                }
            }
            for (Integer sampleToRemove : samplesToRemove) {
                sc.getSampleIds().inverse().remove(sampleToRemove);
                for (LinkedHashSet<Integer> samplesInFile : sc.getSamplesInFiles().values()) {
                    samplesInFile.remove(sampleToRemove);
                }
                sc.getCohorts().values().forEach(samplesInCohort -> samplesInCohort.remove(sampleToRemove));
            }
            sc.setBatches(Collections.emptyList());

            studyConfigurations.add(sc);
        });

        ExportMetadata exportMetadata = new ExportMetadata(studyConfigurations, query, queryOptions);

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
