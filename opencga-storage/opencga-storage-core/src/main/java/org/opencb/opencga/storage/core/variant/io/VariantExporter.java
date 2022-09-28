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

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
    public static final String TPED_FILE_EXTENSION = ".tped";
    public static final String TFAM_FILE_EXTENSION = ".tfam";
    protected final VariantStorageEngine engine;
    protected final VariantWriterFactory variantWriterFactory;
    protected final VariantMetadataFactory metadataFactory;
    protected final IOConnectorProvider ioConnectorProvider;

    private final Logger logger = LoggerFactory.getLogger(VariantExporter.class);

    public VariantExporter(VariantStorageEngine engine, IOConnectorProvider ioConnectorProvider) throws StorageEngineException {
        this(engine, new VariantMetadataFactory(engine.getMetadataManager()), ioConnectorProvider);
    }

    public VariantExporter(VariantStorageEngine engine, VariantMetadataFactory metadataFactory, IOConnectorProvider ioConnectorProvider)
            throws StorageEngineException {
        this.engine = engine;
        variantWriterFactory = new VariantWriterFactory(engine.getDBAdaptor());
        this.metadataFactory = metadataFactory;
        this.ioConnectorProvider = ioConnectorProvider;
    }

    /**
     * Exports the result of the given query and the associated metadata.
     * @param outputFile    Optional output file. If null or empty, will print into the Standard output. Won't export any metadata.
     * @param outputFormat  Variant Output format.
     * @param variantsFile  Optional variants file.
     * @param query         Query with the variants to export
     * @throws IOException  If there is any IO error
     * @throws StorageEngineException  If there is any error exporting variants
     * @return output file
     */
    public URI export(@Nullable URI outputFile, VariantOutputFormat outputFormat, URI variantsFile,
                      ParsedVariantQuery query)
            throws IOException, StorageEngineException {

        outputFile = VariantWriterFactory.checkOutput(outputFile, outputFormat);
        if (!VariantWriterFactory.isStandardOutput(outputFile)) {
            ioConnectorProvider.checkWritable(outputFile);
        }

        try (OutputStream os = VariantWriterFactory.getOutputStream(outputFile, outputFormat, ioConnectorProvider)) {
            boolean logProgress = !VariantWriterFactory.isStandardOutput(outputFile);
            exportData(outputFile, os, outputFormat, variantsFile, query.getInputQuery(), query.getInputOptions(), logProgress);
        }
        if (metadataFactory != null && !VariantWriterFactory.isStandardOutput(outputFile)) {
            VariantMetadata metadata = metadataFactory.makeVariantMetadata(query.getInputQuery(), query.getInputOptions());
            String metaFilename = outputFile.getPath() + METADATA_FILE_EXTENSION;
            if (outputFormat == VariantOutputFormat.TPED) {
                metaFilename = outputFile.getPath().replace(TPED_FILE_EXTENSION, TFAM_FILE_EXTENSION);
            }
            writeMetadata(metadata, UriUtils.replacePath(outputFile, metaFilename));
        }
        return outputFile;
    }

    protected void exportData(URI outputFile, OutputStream outputStream, VariantOutputFormat outputFormat, URI variantsFile,
                              Query query, QueryOptions queryOptions, boolean logProgress)
            throws StorageEngineException, IOException {
        if (query == null) {
            query = new Query();
        }
        if (queryOptions == null) {
            queryOptions = new QueryOptions();
        }

        // DataReader
        VariantDBReader variantDBReader;
        if (variantsFile != null) {
            Iterator<Variant> variants = toVariantsIterator(variantsFile);
            variantDBReader = new VariantDBReader(engine.iterator(variants, query, queryOptions));
        } else {
            variantDBReader = new VariantDBReader(engine, query, queryOptions);
        }

        // TaskMetadata<Variant, Variant>
        Task<Variant, Variant> progressTask;
        if (logProgress) {
            final Query finalQuery = query;
            final QueryOptions finalQueryOptions = queryOptions;
            ProgressLogger progressLogger = new ProgressLogger("Export variants", () -> {
                if (finalQueryOptions.getBoolean(QueryOptions.SKIP_COUNT) || variantsFile != null) {
                    return 0L;
                }
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
        } else {
            progressTask = batch -> batch;
        }

        // DataWriter
        DataWriter<Variant> variantDataWriter = newVariantDataWriter(outputFile, outputStream, outputFormat, query, queryOptions);

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

    protected DataWriter<Variant> newVariantDataWriter(URI outputFile, OutputStream outputStream, VariantOutputFormat outputFormat,
                                                       Query query, QueryOptions queryOptions) throws IOException {
        return variantWriterFactory.newDataWriter(outputFormat, outputStream, query, queryOptions);
    }

    protected void writeMetadata(VariantMetadata metadata, URI metadataFile) throws IOException {
        if (metadataFile.toString().endsWith(TFAM_FILE_EXTENSION)) {
            // Write .tfam file
            writeTfam(metadata, metadataFile);
        } else {
            ObjectMapper objectMapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
            try (OutputStream os = ioConnectorProvider.newOutputStream(metadataFile)) {
                objectMapper.writeValue(os, metadata);
            }
        }
    }

    private void writeTfam(VariantMetadata metadata, URI metadataFile) throws IOException {
        OutputStream os = ioConnectorProvider.newOutputStream(metadataFile);
        Writer writer = new OutputStreamWriter(new BufferedOutputStream(os));
        for (Individual individual : metadata.getStudies().get(0).getIndividuals()) {
            // Sex code: '1' = male, '2' = female, '0' = unknown
            int sex = 0;
            if (individual.getSex() != null) {
                switch (individual.getSex()) {
                    case "MALE": {
                        sex = 1;
                        break;
                    }
                    case "FEMALE": {
                        sex = 2;
                        break;
                    }
                    default: {
                        sex = 0;
                        break;
                    }
                }
            }
            // Phenotype value: '1' = control, '2' = case, '-9'/'0'/non-numeric = missing data if case/control
            int phenotype = 0;

            writer.write((individual.getFamily() == null ? "0" : individual.getFamily())
                    + "\t" + individual.getId()
                    + "\t" + (individual.getFather() == null ? "0" : individual.getFather())
                    + "\t" + (individual.getMother() == null ? "0" : individual.getMother())
                    + "\t" + sex
                    + "\t" + phenotype
                    + "\n");

        }
        writer.close();
        os.close();
    }

    private Iterator<Variant> toVariantsIterator(URI variantsFile) {
        VariantStudyMetadata metadata = new VariantFileMetadata("", variantsFile.getPath()).toVariantStudyMetadata("");
        return new VariantVcfReader(metadata, variantsFile.getPath(),
                (variantStudyMetadata, s) -> {
                    String[] split = s.split("\t");
                    if (split.length < 5) {
                        throw new IllegalArgumentException("Not enough fields provided (min 5)");
                    }
                    return Collections.singletonList(new Variant(split[0], Integer.valueOf(split[1]), split[3], split[4]));
                }).iterator();
    }

}
