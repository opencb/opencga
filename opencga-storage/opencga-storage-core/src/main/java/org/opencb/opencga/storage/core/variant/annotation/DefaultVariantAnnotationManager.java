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
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.io.avro.AvroDataReader;
import org.opencb.commons.io.avro.AvroDataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationSaveParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleAnnotationIndexer;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.db.VariantAnnotationDBWriter;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantAnnotationJsonDataReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantAnnotationJsonDataWriter;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.opencb.biodata.models.core.Region.normalizeChromosome;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.*;

/**
 * Two steps annotation pipeline.
 * Defines the steps create and load.
 *
 * Created by jacobo on 9/01/15.
 *
 * TODO: Make this class abstract
 * @author Javier Lopez &lt;fjlopez@ebi.ac.uk&gt;
 */
public class DefaultVariantAnnotationManager extends VariantAnnotationManager {

    public static final String FILE_NAME = "fileName";
    public static final String OUT_DIR = "outDir";

    protected VariantDBAdaptor dbAdaptor;
    protected VariantAnnotator variantAnnotator;
    private final AtomicLong numProcessedVariants = new AtomicLong(0);
    private final AtomicLong numAnnotationsToLoad = new AtomicLong(0);
    protected static Logger logger = LoggerFactory.getLogger(DefaultVariantAnnotationManager.class);
    protected Map<Integer, List<Integer>> filesToBeAnnotated = new HashMap<>();
    protected Map<Integer, Collection<Integer>> samplesToBeAnnotated = new HashMap<>();
    protected Map<Integer, Collection<Integer>> alreadyAnnotatedSamples = new HashMap<>();
    /**
     * Subset of {@link #alreadyAnnotatedSamples} whose SSI annotationSetId stamp lags the project
     * current - populated during preAnnotate using the same SampleMetadata reads as the file/sample
     * partition, so updateSampleIndexAnnotation doesn't need a second round-trip per sample.
     */
    protected Map<Integer, Collection<Integer>> samplesWithLaggingSsiStamp = new HashMap<>();
    protected Map<Integer, List<Integer>> alreadyAnnotatedFiles = new HashMap<>();
    private final IOConnectorProvider ioConnectorProvider;
    private final VariantReaderUtils variantReaderUtils;
    private boolean annotateAll;
    private long annotationStartTimestamp;
    /**
     * Project's annotationSetId snapshot taken once at the top of {@link #preAnnotate}, right
     * after {@code checkCurrentAnnotation} (called from {@code annotate()} before us) may have
     * bumped it. preAnnotate's partition, the annotation loop, postAnnotate, and
     * {@link #updateSampleIndexAnnotation} all consume this value rather than re-reading from
     * the metadata manager - guarantees the whole pipeline sees a consistent id even if another
     * process bumps the project mid-run.
     */
    private int currentAnnotationSetId;
    protected Query query;
    private final SampleAnnotationIndexer sampleIndexAnnotation;

    public DefaultVariantAnnotationManager(VariantAnnotator variantAnnotator, VariantDBAdaptor dbAdaptor,
                                           IOConnectorProvider ioConnectorProvider) {
        this(variantAnnotator, dbAdaptor, ioConnectorProvider, null);
    }

    public DefaultVariantAnnotationManager(VariantAnnotator variantAnnotator, VariantDBAdaptor dbAdaptor,
                                           IOConnectorProvider ioConnectorProvider,
                                           SampleAnnotationIndexer sampleIndexAnnotation) {
        Objects.requireNonNull(variantAnnotator);
        Objects.requireNonNull(dbAdaptor);
        this.dbAdaptor = dbAdaptor;
        this.variantAnnotator = variantAnnotator;
        this.sampleIndexAnnotation = sampleIndexAnnotation;
        this.ioConnectorProvider = ioConnectorProvider;
        variantReaderUtils = new VariantReaderUtils(this.ioConnectorProvider);
    }

    @Override
    public long annotate(Query query, ObjectMap params) throws VariantAnnotatorException, IOException, StorageEngineException {
        String annotationFileStr = params.getString(LOAD_FILE);
        boolean doCreate = params.getBoolean(CREATE);
        boolean doLoad = StringUtils.isNotEmpty(annotationFileStr);
        if (!doCreate && !doLoad) {
            doCreate = true;
            doLoad = true;
        }
        boolean overwrite = params.getBoolean(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), false);
        boolean forceNewAnnotationSet = params.getBoolean(VariantStorageOptions.ANNOTATION_FORCE_NEW_ANNOTATION_SET.key(),
                VariantStorageOptions.ANNOTATION_FORCE_NEW_ANNOTATION_SET.defaultValue());
        if (overwrite) {
            query.remove(VariantQueryParam.ANNOTATION_EXISTS.key());
        } else {
            query.put(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
        }
        this.query = query;
        int checkpointSize = params.getInt(
                VariantStorageOptions.ANNOTATION_CHECKPOINT_SIZE.key(),
                VariantStorageOptions.ANNOTATION_CHECKPOINT_SIZE.defaultValue());

        // checkCurrentAnnotation may bump the project's annotationSetId (when the annotator,
        // dataRelease, extensions, etc. have changed under overwrite=true, or when
        // forceNewAnnotationSet=true). This must run BEFORE preAnnotate, because preAnnotate's
        // discovery loop partitions indexed files/samples by whether their stored annotationSetId
        // matches the project's *current* one - if we ran preAnnotate first it would see the
        // pre-bump value and treat about-to-become-stale rows as fresh.
        if (doCreate && doLoad) {
            ProjectMetadata.VariantAnnotationMetadata newVariantAnnotationMetadata = variantAnnotator.getVariantAnnotationMetadata();

            dbAdaptor.getMetadataManager().updateProjectMetadata(projectMetadata -> {
                checkCurrentAnnotation(projectMetadata, overwrite, forceNewAnnotationSet, newVariantAnnotationMetadata);
                return projectMetadata;
            });
        }

        preAnnotate(query, doCreate, doLoad, params);

        long variants = 0;
        if (checkpointSize > 0 && doLoad && doCreate && doBatchAnnotation(params)) {
            int batch = 0;
            long processedVariants;
            do {
                batch++;
                annotateBatch(query, params, doCreate, doLoad, annotationFileStr, checkpointSize, batch, "." + batch);
                processedVariants = numProcessedVariants.get();
                variants += numAnnotationsToLoad.get();
            } while (processedVariants == checkpointSize);
        } else {
            variants = annotateBatch(query, params, doCreate, doLoad, annotationFileStr, null, null, null);
        }
        logger.info("Finish annotation. New annotated variants: " + variants);
        long nonAnnotatedVariants = numProcessedVariants.get() - numAnnotationsToLoad.get();
        if (nonAnnotatedVariants != 0) {
            logger.warn("There were " + nonAnnotatedVariants + " variants that could not be annotated!");
        }

        postAnnotate(query, doCreate, doLoad, params);

        return variants;
    }

    protected boolean doBatchAnnotation(ObjectMap params) {
        // Can't run in batches if overwrite=true, as every batch would be starting over and over
        boolean overwrite = params.getBoolean(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), false);
        return !overwrite;
    }

    protected long annotateBatch(Query query, ObjectMap params, boolean doCreate, boolean doLoad,
                                 String annotationFileStr, Integer batchSize, Integer batchId, String fileSufix)
            throws VariantAnnotatorException, IOException, StorageEngineException {
        numAnnotationsToLoad.set(0);
        numProcessedVariants.set(0);
        URI annotationFile;
        if (doCreate) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation creation" + (batchId == null ? "" : (", batch " + batchId + "x" + batchSize)));
            logger.info("Query : {} ", query.toJson());
            if (batchSize != null && batchSize > 0) {
                params.put(QueryOptions.LIMIT, batchSize);
            }
            annotationFile = createAnnotation(
                    UriUtils.createDirectoryUriSafe(params.getString(OUT_DIR)),
                    params.getString(FILE_NAME, "annotation_" + TimeUtils.getTime()) + fileSufix,
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
            logger.info("Starting annotation load {}", annotationFile);
            loadAnnotation(annotationFile, params);
            logger.info("Finished annotation load {}ms", System.currentTimeMillis() - start);

        }

        return numAnnotationsToLoad.get();
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
    public URI createAnnotation(URI outDir, String fileName, Query query, ObjectMap params) throws VariantAnnotatorException {
        if (params == null) {
            params = new ObjectMap();
        }

        boolean avro = params.getString(VariantStorageOptions.ANNOTATION_FILE_FORMAT.key(),
                VariantStorageOptions.ANNOTATION_FILE_FORMAT.defaultValue()).equalsIgnoreCase("avro");

        URI fileUri = outDir.resolve(fileName + ".annot" + (avro ? ".avro" : ".json") + (".gz"));

        /** Getting iterator from OpenCGA Variant database. **/
        QueryOptions iteratorQueryOptions = getIteratorQueryOptions(query, params);

        int batchSize = params.getInt(
                VariantStorageOptions.ANNOTATION_BATCH_SIZE.key(),
                VariantStorageOptions.ANNOTATION_BATCH_SIZE.defaultValue());
        int numThreads = params.getInt(
                VariantStorageOptions.ANNOTATION_THREADS.key(),
                VariantStorageOptions.ANNOTATION_THREADS.defaultValue());
        int timeoutSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(params.getInt(
                VariantStorageOptions.ANNOTATION_TIMEOUT.key(),
                VariantStorageOptions.ANNOTATION_TIMEOUT.defaultValue()));

        try {
            DataReader<Variant> variantDataReader = getVariantDataReader(query, iteratorQueryOptions, params);
            ProgressLogger progressLogger;
            if (params.getBoolean(QueryOptions.SKIP_COUNT, false)) {
                progressLogger = new ProgressLogger("Annotated variants:", iteratorQueryOptions.getLong(QueryOptions.LIMIT, 0), 200);
            } else {
                ObjectMap finalParams = params;
                progressLogger = new ProgressLogger("Annotated variants:", () -> {
                    long count = countVariantsToAnnotate(query, finalParams);
                    long limit = iteratorQueryOptions.getLong(QueryOptions.LIMIT, 0);
                    if (limit > 0) {
                        return Math.min(limit, count);
                    } else {
                        return count;
                    }
                }, 200);
            }
            Task<Variant, VariantAnnotation> annotationTask = Task
                    .tee(variantAnnotator, variants -> {
                        progressLogger.increment(variants.size(),
                                () -> {
                                    Variant variant = variants.get(variants.size() - 1);
                                    return ", up to position " + variant.toString();
                                });
                        numProcessedVariants.addAndGet(variants.size());
                        return null;
                    })
                    .then((List<VariantAnnotation> variants) -> {
                        numAnnotationsToLoad.addAndGet(variants.size());
                        return variants;
                    });

            final DataWriter<VariantAnnotation> variantAnnotationDataWriter;
            if (avro) {
                //FIXME
                variantAnnotationDataWriter = new AvroDataWriter<>(null, true, VariantAnnotation.getClassSchema());
            } else {
                try {
                    variantAnnotationDataWriter = new VariantAnnotationJsonDataWriter(ioConnectorProvider.newOutputStream(fileUri));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                    .setNumTasks(numThreads)
                    .setBatchSize(batchSize)
                    .setAbortOnFail(true)
                    .setReadQueuePutTimeout(timeoutSeconds, TimeUnit.SECONDS)
                    .setSorted(false).build();
            ParallelTaskRunner<Variant, VariantAnnotation> parallelTaskRunner =
                    new ParallelTaskRunner<>(variantDataReader, annotationTask, variantAnnotationDataWriter, config);
            parallelTaskRunner.run();
        } catch (Exception e) {
            throw new VariantAnnotatorException("Error creating annotations", e);
        }

        return fileUri;
    }

    protected DataReader<Variant> getVariantDataReader(Query query, QueryOptions iteratorQueryOptions, ObjectMap params) {
        return new VariantDBReader(dbAdaptor, query, iteratorQueryOptions);
    }

    protected long countVariantsToAnnotate(Query query, ObjectMap params) {
        return dbAdaptor.count(query).first();
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
        if (isCustomAnnotation(uri)) {
            loadCustomAnnotation(uri, params);
        } else {
            loadVariantAnnotation(uri, params);
        }
    }

    protected boolean isCustomAnnotation(URI uri) {
        String fileName = UriUtils.fileName(uri);
        return !VariantReaderUtils.isAvro(fileName) && !VariantReaderUtils.isJson(fileName);
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
        boolean fileDeleteAfterLoad = params.getBoolean(VariantStorageOptions.ANNOTATION_FILE_DELETE_AFTER_LOAD.key(),
                VariantStorageOptions.ANNOTATION_FILE_DELETE_AFTER_LOAD.defaultValue());
        final int batchSize = params.getInt(
                VariantStorageOptions.ANNOTATION_LOAD_BATCH_SIZE.key(),
                VariantStorageOptions.ANNOTATION_LOAD_BATCH_SIZE.defaultValue());
        final int numConsumers = params.getInt(
                VariantStorageOptions.ANNOTATION_LOAD_THREADS.key(),
                VariantStorageOptions.ANNOTATION_LOAD_THREADS.defaultValue());

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
            if (fileDeleteAfterLoad) {
                logger.info("Delete temporary file after loading");
                Files.delete(Paths.get(uri));
            }
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

    protected DataReader<VariantAnnotation> newVariantAnnotationDataReader(URI uri) throws IOException {
        DataReader<VariantAnnotation> reader;
        if (VariantReaderUtils.isAvro(uri.toString())) {
            // FIXME
            reader = new AvroDataReader<>(Paths.get(uri).toFile(), VariantAnnotation.class);
        } else if (VariantReaderUtils.isJson(uri.toString())) {
            reader = new VariantAnnotationJsonDataReader(ioConnectorProvider.newInputStream(uri));
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
     * Populates the list of {@link #filesToBeAnnotated}.
     *
     * Determine if this query is going to annotate all the variants from the database.
     * If so, list all the currently indexed files. These will be marked as "annotated" once the annotation is loaded.
     *
     * @see #postAnnotate
     * @param query            Query for creating the annotation.
     * @param doCreate         if creating the annotation
     * @param doLoad           if loading the annotation
     * @param params           Other annotation params
     * @throws StorageEngineException if an error occurs
     * @throws VariantAnnotatorException if an error occurs
     */
    protected void preAnnotate(Query query, boolean doCreate, boolean doLoad, ObjectMap params)
            throws StorageEngineException, VariantAnnotatorException {
        if (!doCreate || !doLoad) {
            // Do not continue if loading an external annotation file, or not loading it.
            return;
        }

        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        // Snapshot the project's current annotationSetId once for the whole pass. checkCurrentAnnotation
        // (called from annotate() before us) may have just bumped it; everything downstream - the
        // partition below, postAnnotate, updateSampleIndexAnnotation - must read this field rather
        // than re-fetching from the metadata manager, so the pipeline is internally consistent
        // even if another process bumps the project mid-run.
        currentAnnotationSetId = metadataManager.getProjectMetadata().getAnnotation().getCurrent().getId();
        Set<VariantQueryParam> queryParams = VariantQueryUtils.validParams(query, true);
        Set<String> filesFilter = Collections.emptySet();
        queryParams.removeAll(Arrays.asList(VariantQueryParam.ANNOTATION_EXISTS, VariantQueryParam.STUDY));

        annotationStartTimestamp = System.currentTimeMillis();
        if (queryParams.isEmpty()) {
            // There are no invalid filters.
            annotateAll = true;
        } else if (queryParams.size() == 1 && queryParams.contains(VariantQueryParam.FILE)) {
            // Annotating some files entirely
            filesFilter = new HashSet<>(query.getAsStringList(VariantQueryParam.FILE.key()));

            annotateAll = true;
            for (String file : filesFilter) {
                if (VariantQueryUtils.isNegated(file) || file.contains(VariantQueryUtils.AND)) {
                    // Invalid file filter
                    annotateAll = false;
                    break;
                }
            }
        } else {
            // There are filters like REGION. With this filter we can not guarantee that all the variants from any file will be annotated
            annotateAll = false;
        }

        if (annotateAll) {
            // A file/sample is "fresh" iff its annotationStatus is READY *and* its annotationSetId
            // matches the project's current id. annotationSetId == 0 means "unstamped" and is
            // treated as fresh for backwards compatibility (existing projects pre-dating this field
            // would otherwise trigger a mass re-annotation on the first run).
            List<Integer> studies = VariantQueryProjectionParser.getIncludeStudies(query, null, metadataManager);
            for (Integer studyId : studies) {
                List<Integer> files = new LinkedList<>();
                Collection<Integer> samples;
                List<Integer> annotatedFiles = new LinkedList<>();
                Collection<Integer> annotatedSamples = new LinkedList<>();
                Collection<Integer> laggingSsiSamples = new LinkedList<>();
                if (!filesFilter.isEmpty()) {
                    samples = new HashSet<>();
                    annotatedSamples = new HashSet<>();
                    laggingSsiSamples = new HashSet<>();
                    for (String file : filesFilter) {
                        FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, file);
                        if (fileMetadata != null) {
                            if (fileMetadata.isIndexed()) {
                                if (isVariantAnnotationFresh(fileMetadata, currentAnnotationSetId)) {
                                    annotatedFiles.add(fileMetadata.getId());
                                } else {
                                    files.add(fileMetadata.getId());
                                }
                            }
                            for (Integer sample : fileMetadata.getSamples()) {
                                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sample);
                                if (sampleMetadata.isIndexed()) {
                                    if (isVariantAnnotationFresh(sampleMetadata, currentAnnotationSetId)) {
                                        annotatedSamples.add(sample);
                                        if (!isSampleIndexAnnotationFresh(sampleMetadata, currentAnnotationSetId)) {
                                            laggingSsiSamples.add(sample);
                                        }
                                    } else {
                                        samples.add(sample);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    samples = new LinkedList<>();
                    Collection<Integer> annotatedSamplesRef = annotatedSamples;
                    Collection<Integer> laggingSsiSamplesRef = laggingSsiSamples;
                    metadataManager.fileMetadataIterator(studyId).forEachRemaining(fileMetadata -> {
                        if (fileMetadata.isIndexed()) {
                            if (isVariantAnnotationFresh(fileMetadata, currentAnnotationSetId)) {
                                annotatedFiles.add(fileMetadata.getId());
                            } else {
                                files.add(fileMetadata.getId());
                            }
                        }
                    });
                    Collection<Integer> samplesRef = samples;
                    metadataManager.sampleMetadataIterator(studyId).forEachRemaining(sampleMetadata -> {
                        if (sampleMetadata.isIndexed()) {
                            if (isVariantAnnotationFresh(sampleMetadata, currentAnnotationSetId)) {
                                annotatedSamplesRef.add(sampleMetadata.getId());
                                if (!isSampleIndexAnnotationFresh(sampleMetadata, currentAnnotationSetId)) {
                                    laggingSsiSamplesRef.add(sampleMetadata.getId());
                                }
                            } else {
                                samplesRef.add(sampleMetadata.getId());
                            }
                        }
                    });
                }
                filesToBeAnnotated.put(studyId, files);
                samplesToBeAnnotated.put(studyId, samples);
                alreadyAnnotatedFiles.put(studyId, annotatedFiles);
                alreadyAnnotatedSamples.put(studyId, annotatedSamples);
                samplesWithLaggingSsiStamp.put(studyId, laggingSsiSamples);
            }
            logger.info("Annotating {} files and {} samples (annotationSetId target = {})",
                    filesToBeAnnotated.values().stream().mapToInt(Collection::size).sum(),
                    samplesToBeAnnotated.values().stream().mapToInt(Collection::size).sum(),
                    currentAnnotationSetId
            );
        }
    }

    /**
     * Returns {@code true} iff the file/sample's variant-level annotation is at the project's
     * current generation. Backcompat: a stored value of {@code 0} is treated as fresh so projects
     * pre-dating this field do not trigger a mass re-annotation on first run.
     */
    private static boolean isVariantAnnotationFresh(SampleMetadata sampleMetadata, int currentSetId) {
        return isVariantAnnotationFresh(sampleMetadata.isAnnotated(), sampleMetadata.getAnnotationSetId(), currentSetId);
    }

    private static boolean isVariantAnnotationFresh(FileMetadata fileMetadata, int currentSetId) {
        return isVariantAnnotationFresh(fileMetadata.isAnnotated(), fileMetadata.getAnnotationSetId(), currentSetId);
    }

    private static boolean isVariantAnnotationFresh(boolean isAnnotated, int storedSetId, int currentSetId) {
        return isAnnotated && (storedSetId == 0 || storedSetId == currentSetId);
    }

    /**
     * Returns {@code true} iff the sample's Sample Index annotation stamp is at the project's
     * current generation (or the sample has no SSI version, in which case there is no SSI work
     * to consider). Backcompat: stored {@code 0} means "unknown / not stamped yet" and is treated
     * as fresh so projects pre-dating this field do not trigger a mass SSI rebuild on first run.
     *
     * <p>Polarity mirrors {@link #isVariantAnnotationFresh}: both report whether the corresponding
     * annotation layer is up to date. Call sites that want to detect lagging samples negate the
     * result.
     */
    private static boolean isSampleIndexAnnotationFresh(SampleMetadata sampleMetadata, int currentSetId) {
        Integer ssiVersion = sampleMetadata.getSampleIndexAnnotationVersion();
        if (ssiVersion == null) {
            return true;
        }
        int stored = sampleMetadata.getSampleIndexAnnotationSetId(ssiVersion);
        return stored == 0 || stored == currentSetId;
    }

    /**
     * Mark all {@link #filesToBeAnnotated} as annotated.
     *
     * @param query annotation query
     * @param doCreate doCreate
     * @param doLoad doLoad
     * @param params params
     * @see #preAnnotate
     * @throws VariantAnnotatorException on error writing the metadata
     * @throws StorageEngineException on error writing the metadata
     * @throws IOException on error writing the metadata
     */
    protected void postAnnotate(Query query, boolean doCreate, boolean doLoad, ObjectMap params)
            throws VariantAnnotatorException, StorageEngineException, IOException {
        boolean overwrite = params.getBoolean(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), false);
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        if (doLoad) {
            dbAdaptor.getMetadataManager().updateProjectMetadata(projectMetadata -> {
                projectMetadata.setAnnotationIndexLastUpdateEndTimestamp(System.currentTimeMillis());
                return projectMetadata;
            });
        }
        if (doLoad && doCreate) {
            ProjectMetadata.VariantAnnotationMetadata newAnnotationMetadata = variantAnnotator.getVariantAnnotationMetadata();
            boolean forceNewAnnotationSet = params.getBoolean(VariantStorageOptions.ANNOTATION_FORCE_NEW_ANNOTATION_SET.key(),
                    VariantStorageOptions.ANNOTATION_FORCE_NEW_ANNOTATION_SET.defaultValue());

            metadataManager.updateProjectMetadata(projectMetadata -> {
                updateCurrentAnnotation(variantAnnotator, projectMetadata, overwrite, forceNewAnnotationSet, newAnnotationMetadata);
            });
            metadataManager.updateAnnotationIndexTimestamp(annotateAll, annotationStartTimestamp);
        }

        if (doLoad && filesToBeAnnotated != null) {
            // currentAnnotationSetId snapshot taken once at the top of preAnnotate - see field doc.

            for (Map.Entry<Integer, Collection<Integer>> entry : samplesToBeAnnotated.entrySet()) {
                Integer studyId = entry.getKey();
                Collection<Integer> sampleIds = entry.getValue();
                for (Integer sampleId : sampleIds) {
                    metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                        sampleMetadata.setAnnotationStatus(TaskMetadata.Status.READY);
                        sampleMetadata.setAnnotationSetId(currentAnnotationSetId);
                    });
                }
            }

            for (Map.Entry<Integer, List<Integer>> entry : filesToBeAnnotated.entrySet()) {
                Integer studyId = entry.getKey();
                List<Integer> fileIds = entry.getValue();

                for (Integer file : fileIds) {
                    metadataManager.updateFileMetadata(studyId, file, fileMetadata -> {
                        fileMetadata.setAnnotationStatus(TaskMetadata.Status.READY);
                        fileMetadata.setAnnotationSetId(currentAnnotationSetId);
                    });
                }
            }
        }

        if (sampleIndexAnnotation != null) {
            updateSampleIndexAnnotation(params);
        }
    }

    protected void updateSampleIndexAnnotation(ObjectMap params) throws IOException, StorageEngineException {
        boolean sampleIndex = YesNoAuto.parse(params, VariantStorageOptions.ANNOTATION_SAMPLE_INDEX.key()).booleanValue(true);
        if (!sampleIndex) {
            logger.info("Skip Sample Index Annotation");
            return;
        }
        if (!annotateAll) {
            // Partial annotation pass (region/sample/etc. filter): SSI work is per-sample-whole
            // and would rebuild from a mix of fresh-and-stale variants - the work scope doesn't
            // match the variant-filter scope. Defer SSI sync to a full annotation pass or to an
            // explicit variant-secondary-sample-index --annotate run.
            logger.info("Skip Sample Index Annotation: partial annotation pass");
            return;
        }

        // Both inputs were populated during preAnnotate with the same SampleMetadata reads used
        // for the file/sample partition - no extra metadata round-trips needed here.
        List<Integer> studies = VariantQueryProjectionParser.getIncludeStudies(query, null,
                dbAdaptor.getMetadataManager());
        for (Integer studyId : studies) {
            Set<Integer> samplesToUpdate = new HashSet<>();
            // Variants were re-annotated → SSI must follow.
            samplesToUpdate.addAll(samplesToBeAnnotated.getOrDefault(studyId, Collections.emptyList()));
            // Variants were already fresh but the SSI stamp lags the project current.
            samplesToUpdate.addAll(samplesWithLaggingSsiStamp.getOrDefault(studyId, Collections.emptyList()));

            if (!samplesToUpdate.isEmpty()) {
                // Every sample in this set was deliberately chosen because its SSI is known to
                // need rebuild. Force overwrite=true so the indexer's "skip if READY" safety net -
                // intended for direct CLI re-runs - doesn't no-op this orchestrated rebuild.
                sampleIndexAnnotation.updateSampleAnnotation(studyId, new ArrayList<>(samplesToUpdate), params, true);
            }
        }
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

        final int batchSize = params.getInt(
                VariantStorageOptions.ANNOTATION_LOAD_BATCH_SIZE.key(),
                VariantStorageOptions.ANNOTATION_LOAD_BATCH_SIZE.defaultValue());
        final int numConsumers = params.getInt(
                VariantStorageOptions.ANNOTATION_LOAD_THREADS.key(),
                VariantStorageOptions.ANNOTATION_LOAD_THREADS.defaultValue());
        final String key = params.getString(CUSTOM_ANNOTATION_KEY, "default");
        long ts = System.currentTimeMillis();

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(numConsumers)
                .setBatchSize(batchSize)
                .setAbortOnFail(true)
                .setSorted(false)
                .build();


        Path path = Paths.get(uri);
        String fileName = UriUtils.fileName(uri).toLowerCase();
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
                                        query, key, new AdditionalAttribute(Collections.singletonMap("feature", gff.getFeature())), ts,
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
                                AdditionalAttribute additionalAttribute = new AdditionalAttribute(annotation);
                                dbAdaptor.updateCustomAnnotations(query, key, additionalAttribute, ts, QueryOptions.empty());
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
            VariantStudyMetadata metadata = new VariantFileMetadata(fileName, fileName).toVariantStudyMetadata("s");
            ParallelTaskRunner<Variant, Void> ptr = new ParallelTaskRunner<>(
                    variantReaderUtils.getVariantVcfReader(path, metadata),
                    variantList -> {
                        for (Variant variant : variantList) {
                            Region region = new Region(normalizeChromosome(variant.getChromosome()), variant.getStart(), variant.getEnd());
                            Query query = new Query(VariantQueryParam.REGION.key(), region);
                            Map<String, String> info = variant.getStudies().get(0).getFiles().get(0).getData();
                            AdditionalAttribute attribute = new AdditionalAttribute(info);
                            dbAdaptor.updateCustomAnnotations(query, key, attribute, ts, new QueryOptions());
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

    //TODO: Make this method abstract
    @Override
    public void saveAnnotation(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException {
        throw new UnsupportedOperationException();
    }

    /**
     * Shared metadata side of {@code saveAnnotation} for both Mongo and Hadoop backends. Atomically
     * routes through the right primitive based on the {@code fromAnnotationSet} option:
     *
     * <ul>
     *   <li>{@code fromAnnotationSet} is empty / {@code null} / {@code "current"} →
     *       {@link #registerNewAnnotationSnapshot} (autobump current state, immediately promote to
     *       saved under {@code name}).</li>
     *   <li>{@code fromAnnotationSet} is a transition's auto-name →
     *       {@link #promoteTransitionToSaved} (move the named transition from {@code transitions}
     *       to {@code saved} under {@code name}; preserves the transition's original id).</li>
     * </ul>
     *
     * <p>Idempotent on partial-failure retry: if {@code name} is already present in {@code saved}
     * (e.g. a previous attempt's metadata step landed but the data copy crashed), the metadata is
     * left untouched and the existing entry's id is returned. The caller then re-runs the
     * backend-specific data copy, which is naturally idempotent on both backends (Mongo
     * {@code $out} replaces the target collection, Hadoop overwrites the snapshot column). When
     * an explicit {@code fromAnnotationSet} is given, the existing saved entry's id must match
     * that transition's id; a mismatch is treated as a real name collision and rejected.
     *
     * @param name    operator-given snapshot name to land in {@code saved}
     * @param options job options; reads {@link VariantAnnotationSaveParams#FROM_ANNOTATION_SET}
     * @return the saved entry's annotationSetId - backends use this to drive their per-id variant
     *         data copy (Mongo {@code $match} aggregation, Hadoop scan filter on {@code A_ID}).
     * @throws StorageEngineException     if the metadata mutation fails
     * @throws VariantAnnotatorException  if the snapshot name collides with a different generation
     *                                    or the named transition is not found
     */
    protected final int updateProjectMetadataForSaveAnnotation(String name, ObjectMap options)
            throws StorageEngineException, VariantAnnotatorException {
        String fromAnnotationSet = options.getString(VariantAnnotationSaveParams.FROM_ANNOTATION_SET);
        AtomicInteger snapshotIdRef = new AtomicInteger();
        dbAdaptor.getMetadataManager().updateProjectMetadata(project -> {
            // Idempotent retry path. If `name` already exists in saved, a previous saveAnnotation
            // call already promoted the entry - but the data copy may have failed (MR crash, JVM
            // death, network blip during Mongo aggregation). Skip the metadata mutation and return
            // the existing id so the backend re-runs the data copy.
            ProjectMetadata.VariantAnnotationMetadata existing = project.getAnnotation().getSavedOrNull(name);
            if (existing != null) {
                // Defence against accidental name reuse for a semantically different generation:
                // when the caller explicitly named a source transition, the existing saved entry
                // must come from that same transition (matching id). Otherwise the operator is
                // trying to overload the name for a different generation - refuse. If the
                // transition is missing, it likely IS the one we already promoted on the prior
                // attempt (promoteTransitionToSaved removes it from transitions), so absence is
                // consistent with a retry - do not reject.
                if (StringUtils.isNotEmpty(fromAnnotationSet)
                        && !VariantAnnotationManager.CURRENT.equalsIgnoreCase(fromAnnotationSet)) {
                    ProjectMetadata.VariantAnnotationMetadata source = project.getAnnotation()
                            .getTransitionOrNull(fromAnnotationSet);
                    if (source != null && source.getId() != existing.getId()) {
                        throw new VariantAnnotatorException("Annotation snapshot name '" + name
                                + "' already exists with id=" + existing.getId()
                                + " but the requested source transition '" + fromAnnotationSet
                                + "' has id=" + source.getId()
                                + " - reusing the snapshot name for a different generation is not allowed.");
                    }
                }
                logger.info("saveAnnotation '{}' already exists in saved (id={}); skipping metadata mutation, "
                        + "re-running data copy only.", name, existing.getId());
                snapshotIdRef.set(existing.getId());
            } else {
                ProjectMetadata.VariantAnnotationMetadata snap;
                if (StringUtils.isEmpty(fromAnnotationSet)
                        || VariantAnnotationManager.CURRENT.equalsIgnoreCase(fromAnnotationSet)) {
                    snap = registerNewAnnotationSnapshot(name, variantAnnotator, project);
                } else {
                    snap = promoteTransitionToSaved(fromAnnotationSet, name, project);
                }
                snapshotIdRef.set(snap.getId());
            }
            return project;
        });
        return snapshotIdRef.get();
    }

    //TODO: Make this method abstract
    @Override
    public void deleteAnnotation(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String synchroniseWithCurrentAnnotator(ProjectMetadata.VariantAnnotationMetadata preResolvedMetadata,
                                                  ProjectMetadata.VariantAnnotatorProgram expectedCurrentAnnotator)
            throws StorageEngineException, VariantAnnotatorException {
        // Track the current.id BEFORE entering the lock. If updateCurrentAnnotation triggers a
        // bump, current.id increments by 1; the OLD id ends up as the id of the freshly recorded
        // transition. Using an id delta (rather than a list-size delta) correctly handles the
        // idempotent dedup branch inside bumpAnnotationSetId - that path returns an existing
        // entry without appending, but also without incrementing current.id, so an id-delta of 0
        // accurately means "no bump made by THIS call".
        int[] preBumpId = {0};
        boolean[] casAborted = {false};
        ProjectMetadata updatedPm = dbAdaptor.getMetadataManager().updateProjectMetadata((ProjectMetadata pm) -> {
            ProjectMetadata.VariantAnnotatorProgram observedCurrentAnnotator =
                    pm.getAnnotation().getCurrent().getAnnotator();
            if (expectedCurrentAnnotator != null && !Objects.equals(expectedCurrentAnnotator, observedCurrentAnnotator)) {
                // CAS guard: a concurrent updateCellbaseConfiguration already moved current past
                // the value we observed at probe time. Aborting here keeps us from racing in a
                // stale transition that would corrupt the audit trail (e.g. downgrading current
                // back to an older state). The other writer's transition is the authoritative
                // record for this window.
                casAborted[0] = true;
                logger.warn("Aborting synchroniseWithCurrentAnnotator: current.annotator changed between probe and lock acquisition. "
                        + "Expected={}, observed={}.", expectedCurrentAnnotator, observedCurrentAnnotator);
                return pm;
            }
            preBumpId[0] = pm.getAnnotation().getCurrent().getId();
            // updateCurrentAnnotation routes through checkCurrentAnnotation, which compares the
            // new metadata against current field-by-field on annotation-relevant fields and only
            // bumps when a real semantic difference exists. overwrite=true silences the safety
            // throws so a cosmetic-but-real change (e.g. CellBase patch bump) becomes a silent
            // bump rather than an error. forceNewAnnotationSet=false so unchanged configs stay
            // no-op.
            updateCurrentAnnotation(variantAnnotator, pm, true, false, preResolvedMetadata);
            return pm;
        });
        if (casAborted[0]) {
            return null;
        }
        int postBumpId = updatedPm.getAnnotation().getCurrent().getId();
        if (postBumpId == preBumpId[0]) {
            return null;
        }
        // The bump pushed the pre-bump state into transitions tagged with the OLD id. Look it up
        // by id so we return the right entry even if other transitions are interleaved.
        ProjectMetadata.VariantAnnotationMetadata bumped =
                updatedPm.getAnnotation().getTransitionByIdOrNull(preBumpId[0]);
        if (bumped != null) {
            return bumped.getName();
        }
        // Defensive: a bump happened but we couldn't find the matching transition. Should not
        // occur under normal operation - bumpAnnotationSetId always appends the prior state.
        logger.warn("Annotation set id bumped from {} to {} but no transition entry was found for id {}",
                preBumpId[0], postBumpId, preBumpId[0]);
        return null;
    }
}
