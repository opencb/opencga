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

package org.opencb.opencga.storage.core.variant.annotation;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.feature.bed.Bed;
import org.opencb.biodata.formats.feature.bed.io.BedReader;
import org.opencb.biodata.formats.feature.gff.Gff;
import org.opencb.biodata.formats.feature.gff.io.GffReader;
import org.opencb.biodata.formats.io.FormatReaderWrapper;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.io.avro.AvroDataReader;
import org.opencb.commons.io.avro.AvroDataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.db.VariantAnnotationDBWriter;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantAnnotationJsonDataReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantAnnotationJsonDataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.*;

/**
 * Two steps annotation pipeline.
 * Defines the steps create and load.
 *
 * Created by jacobo on 9/01/15.
 *
 * @author Javier Lopez &lt;fjlopez@ebi.ac.uk&gt;
 */
public class DefaultVariantAnnotationManager implements VariantAnnotationManager {

    public static final String FILE_NAME = "fileName";
    public static final String OUT_DIR = "outDir";
    public static final String BATCH_SIZE = "batchSize";
    public static final String NUM_WRITERS = "numWriters";
    public static final String NUM_THREADS = "numThreads";

    protected VariantDBAdaptor dbAdaptor;
    protected VariantAnnotator variantAnnotator;
    private final AtomicLong numAnnotationsToLoad = new AtomicLong(0);
    protected static Logger logger = LoggerFactory.getLogger(DefaultVariantAnnotationManager.class);

    public DefaultVariantAnnotationManager(VariantAnnotator variantAnnotator, VariantDBAdaptor dbAdaptor) {
        if (dbAdaptor == null || variantAnnotator == null) {
            throw new NullPointerException();
        }
        this.dbAdaptor = dbAdaptor;
        this.variantAnnotator = variantAnnotator;
    }

    @Override
    public void annotate(Query query, ObjectMap params) throws VariantAnnotatorException, IOException, StorageEngineException {

        String annotationFileStr = params.getString(LOAD_FILE);
        boolean doCreate = params.getBoolean(CREATE);
        boolean doLoad = StringUtils.isNotEmpty(annotationFileStr);
        if (!doCreate && !doLoad) {
            doCreate = true;
            doLoad = true;
        }

        URI annotationFile;
        if (doCreate) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation creation");
            logger.info("Query : {} ", query.toJson());
            annotationFile = createAnnotation(
                    Paths.get(params.getString(OUT_DIR, "/tmp")),
                    params.getString(FILE_NAME, "annotation_" + TimeUtils.getTime()),
                    query, params);
            logger.info("Finished annotation creation {}ms, generated file {}", System.currentTimeMillis() - start, annotationFile);
        } else {
            try {
                annotationFile = UriUtils.createUri(annotationFileStr);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }

        if (doLoad) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation load");
            loadAnnotation(annotationFile, params);
            logger.info("Finished annotation load {}ms", System.currentTimeMillis() - start);
        }
    }

    /**
     * Creates a variant annotation file from an specific source based on the content of a Variant DataBase.
     *
     * @param outDir   File outdir.
     * @param fileName Generated file name.
     * @param query    Query for those variants to annotate.
     * @param params   Specific params.
     * @return URI of the generated file.
     * @throws VariantAnnotatorException IOException thrown
     */
    public URI createAnnotation(Path outDir, String fileName, Query query, ObjectMap params) throws VariantAnnotatorException {

        boolean gzip = params == null || params.getBoolean("gzip", true);
        boolean avro = params == null || params.getBoolean("annotation.file.avro", false);
        Path path = Paths.get(outDir != null
                ? outDir.toString()
                : "/tmp", fileName + ".annot" + (avro ? ".avro" : ".json") + (gzip ? ".gz" : ""));
        URI fileUri = path.toUri();

        /** Getting iterator from OpenCGA Variant database. **/
        QueryOptions iteratorQueryOptions = getIteratorQueryOptions(query, params);

        int batchSize = 200;
        int numThreads = 8;
        if (params != null) { //Parse query options
            batchSize = params.getInt(BATCH_SIZE, batchSize);
            numThreads = params.getInt(NUM_THREADS, numThreads);
        }

        try {
            DataReader<Variant> variantDataReader = new VariantDBReader(dbAdaptor, query, iteratorQueryOptions);
            ProgressLogger progressLogger;
            if (params != null && params.getBoolean(QueryOptions.SKIP_COUNT, false)) {
                progressLogger = new ProgressLogger("Annotated variants:", iteratorQueryOptions.getLong(QueryOptions.LIMIT, 0), 200);
            } else {
                progressLogger = new ProgressLogger("Annotated variants:", () -> {
                    long limit = iteratorQueryOptions.getLong(QueryOptions.LIMIT, 0);
                    if (limit > 0) {
                        return limit;
                    }
                    return dbAdaptor.count(query).first();
                }, 200);
            }
            ParallelTaskRunner.TaskWithException<Variant, VariantAnnotation, VariantAnnotatorException> annotationTask = variantList -> {
                List<VariantAnnotation> variantAnnotationList;
                long start = System.currentTimeMillis();
                logger.debug("Annotating batch of {} genomic variants.", variantList.size());
                variantAnnotationList = variantAnnotator.annotate(variantList);
                progressLogger.increment(variantList.size(),
                        () -> ", up to position " + variantList.get(variantList.size() - 1).toString());
                numAnnotationsToLoad.addAndGet(variantList.size());

                logger.debug("Annotated batch of {} genomic variants. Time: {}s", variantList.size(),
                        (System.currentTimeMillis() - start) / 1000.0);
                return variantAnnotationList;
            };

            final DataWriter<VariantAnnotation> variantAnnotationDataWriter;
            if (avro) {
                variantAnnotationDataWriter = new AvroDataWriter<>(path, gzip, VariantAnnotation.getClassSchema());
            } else {
                variantAnnotationDataWriter = new VariantAnnotationJsonDataWriter(path, gzip);
            }

            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                    .setNumTasks(numThreads)
                    .setBatchSize(batchSize)
                    .setAbortOnFail(true)
                    .setSorted(false).build();
            ParallelTaskRunner<Variant, VariantAnnotation> parallelTaskRunner =
                    new ParallelTaskRunner<>(variantDataReader, annotationTask, variantAnnotationDataWriter, config);
            parallelTaskRunner.run();
        } catch (ExecutionException e) {
            throw new VariantAnnotatorException("Error creating annotations", e);
        }

        return fileUri;
    }

    protected QueryOptions getIteratorQueryOptions(Query query, ObjectMap params) {
        QueryOptions iteratorQueryOptions;
        if (params == null) {
            iteratorQueryOptions = new QueryOptions();
        } else {
            iteratorQueryOptions = new QueryOptions(params);
        }
        List<VariantField> include = Arrays.asList(CHROMOSOME, START, END, REFERENCE, ALTERNATE, SV);
        iteratorQueryOptions.add(QueryOptions.INCLUDE, include);
        return iteratorQueryOptions;
    }


    public void loadAnnotation(URI uri, ObjectMap params) throws IOException, StorageEngineException {
        Path path = Paths.get(uri);
        String fileName = path.getFileName().toString().toLowerCase();
        if (VariantReaderUtils.isAvro(fileName) || VariantReaderUtils.isJson(fileName)) {
            loadVariantAnnotation(uri, params);
        } else {
            loadCustomAnnotation(uri, params);
        }
    }

    /**
     * Loads variant annotations from an specified file into the selected Variant DataBase.
     *
     * @param uri     URI of the annotation file
     * @param params  Specific params.
     * @throws IOException IOException thrown
     * @throws StorageEngineException if there is a problem creating or running the {@link ParallelTaskRunner}
     */
    public void loadVariantAnnotation(URI uri, ObjectMap params) throws IOException, StorageEngineException {

        final int batchSize = params.getInt(DefaultVariantAnnotationManager.BATCH_SIZE, 100);
        final int numConsumers = params.getInt(DefaultVariantAnnotationManager.NUM_WRITERS, 6);

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(numConsumers)
                .setBatchSize(batchSize)
                .setAbortOnFail(true)
                .setSorted(false).build();
        DataReader<VariantAnnotation> reader;

        reader = newVariantAnnotationDataReader(uri);
        try {
            ProgressLogger progressLogger = new ProgressLogger("Loaded annotations: ", numAnnotationsToLoad.get());
            ParallelTaskRunner<VariantAnnotation, ?> ptr = buildLoadAnnotationParallelTaskRunner(reader, config, progressLogger, params);
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error loading variant annotation", e);
        }

    }

    protected ParallelTaskRunner<VariantAnnotation, ?> buildLoadAnnotationParallelTaskRunner(
            DataReader<VariantAnnotation> reader, ParallelTaskRunner.Config config, ProgressLogger progressLogger, ObjectMap params) {
        return new ParallelTaskRunner<>(reader,
                        () -> newVariantAnnotationDBWriter(dbAdaptor, new QueryOptions(params))
                                .setProgressLogger(progressLogger), null, config);
    }

    protected DataReader<VariantAnnotation> newVariantAnnotationDataReader(URI uri) {
        DataReader<VariantAnnotation> reader;
        if (VariantReaderUtils.isAvro(uri.toString())) {
            reader = new AvroDataReader<>(Paths.get(uri).toFile(), VariantAnnotation.class);
        } else if (VariantReaderUtils.isJson(uri.toString())) {
            reader = new VariantAnnotationJsonDataReader(Paths.get(uri).toFile());
//        } else if (VariantReaderUtils.isVcf(uri.toString())) {
//            //TODO: Read from VEP file
//            reader = new VepFormatReader(Paths.get(uri).toString());
        } else {
            throw new IllegalArgumentException("Unable to load annotations from file " + uri);
        }
        return reader;
    }

    protected VariantAnnotationDBWriter newVariantAnnotationDBWriter(VariantDBAdaptor dbAdaptor, QueryOptions options) {
        return new VariantAnnotationDBWriter(dbAdaptor, options, null);
    }

    /**
     * Loads custom variant annotations from an specified file into the selected Variant DataBase.
     *
     * @param uri     URI of the annotation file
     * @param params  Specific params.
     * @throws IOException IOException thrown
     * @throws StorageEngineException if there is a problem creating or running the {@link ParallelTaskRunner}
     */
    public void loadCustomAnnotation(URI uri, ObjectMap params) throws IOException, StorageEngineException {

        final int batchSize = params.getInt(BATCH_SIZE, 100);
        final int numConsumers = params.getInt(NUM_WRITERS, 6);
        final String key = params.getString(CUSTOM_ANNOTATION_KEY, "default");

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(numConsumers)
                .setBatchSize(batchSize)
                .setAbortOnFail(true)
                .setSorted(false)
                .build();


        Path path = Paths.get(uri);
        String fileName = path.getFileName().toString().toLowerCase();
        if (fileName.endsWith(".gff") || fileName.endsWith(".gff.gz")) {
            try {
                GffReader gffReader = new GffReader(path);
                ParallelTaskRunner<Gff, Void> ptr = new ParallelTaskRunner<>(
                        new FormatReaderWrapper<>(gffReader),
                        gffList -> {
                            for (Gff gff : gffList) {
                                Region region = new Region(normalizeChromosome(gff.getSequenceName()), gff.getStart(), gff.getEnd());
                                Query query = new Query(VariantQueryParam.REGION.key(), region);
                                dbAdaptor.updateCustomAnnotations(
                                        query, key, new AdditionalAttribute(Collections.singletonMap("feature", gff.getFeature())),
                                        QueryOptions.empty());
                            }
                            return Collections.emptyList();
                        }, null, config);

                try {
                    ptr.run();
                } catch (ExecutionException e) {
                    throw new StorageEngineException("Error executing ParallelTaskRunner", e);
                }
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e); // This should never happen!
            }
        } else if (fileName.endsWith(".bed") || fileName.endsWith(".bed.gz")) {
            try {
                BedReader bedReader = new BedReader(path);
                ParallelTaskRunner<Bed, Void> ptr = new ParallelTaskRunner<>(
                        new FormatReaderWrapper<>(bedReader),
                        bedList -> {
                            for (Bed bed: bedList) {
                                Region region = new Region(normalizeChromosome(bed.getChromosome()), bed.getStart(), bed.getEnd());
                                Query query = new Query(VariantQueryParam.REGION.key(), region);
                                Map<String, String> annotation = new HashMap<>(3);
                                annotation.put("name", bed.getName());
                                annotation.put(("score"), String.valueOf(bed.getScore()));
                                annotation.put(("strand"), bed.getStrand());
                                dbAdaptor.updateCustomAnnotations(query, key, new AdditionalAttribute(annotation), QueryOptions.empty());
                            }
                            return Collections.emptyList();
                        }, null, config);
                try {
                    ptr.run();
                } catch (ExecutionException e) {
                    throw new StorageEngineException("Error executing ParallelTaskRunner", e);
                }
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e); // This should never happen!
            }
        } else if (fileName.endsWith(".vcf") || fileName.endsWith(".vcf.gz")) {
            InputStream is = new FileInputStream(path.toFile());
            if (fileName.endsWith(".gz")) {
                is = new GZIPInputStream(is);
            }
            VariantStudyMetadata metadata = new VariantFileMetadata(fileName, fileName).toVariantStudyMetadata("s");
            ParallelTaskRunner<Variant, Void> ptr = new ParallelTaskRunner<>(
                    new VariantVcfHtsjdkReader(is, metadata),
                    variantList -> {
                        for (Variant variant : variantList) {
                            Region region = new Region(normalizeChromosome(variant.getChromosome()), variant.getStart(), variant.getEnd());
                            Query query = new Query(VariantQueryParam.REGION.key(), region);
                            Map<String, String> info = variant.getStudies().get(0).getFiles().get(0).getAttributes();
                            AdditionalAttribute attribute = new AdditionalAttribute(info);
                            dbAdaptor.updateCustomAnnotations(query, key, attribute, new QueryOptions());
                        }
                        return Collections.emptyList();
                    }, null, config);
            try {
                ptr.run();
            } catch (ExecutionException e) {
                throw new StorageEngineException("Error executing ParallelTaskRunner", e);
            }
        } else {
            throw new StorageEngineException("Unknown format file : " + path);
        }
    }

    private String normalizeChromosome(String chromosome) {
        return chromosome.replace("chrom", "").replace("chr", "");
    }


}
