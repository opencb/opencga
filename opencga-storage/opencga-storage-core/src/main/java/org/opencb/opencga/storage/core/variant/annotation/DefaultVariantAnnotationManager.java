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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
    private final AtomicLong numAnnotationsToLoad = new AtomicLong(0);
    protected static Logger logger = LoggerFactory.getLogger(DefaultVariantAnnotationManager.class);
    protected Map<Integer, List<Integer>> filesToBeAnnotated = new HashMap<>();
    protected Map<Integer, Collection<Integer>> samplesToBeAnnotated = new HashMap<>();
    protected Map<Integer, List<Integer>> alreadyAnnotatedFiles = new HashMap<>();
    private final IOConnectorProvider ioConnectorProvider;
    private final VariantReaderUtils variantReaderUtils;

    public DefaultVariantAnnotationManager(VariantAnnotator variantAnnotator, VariantDBAdaptor dbAdaptor,
                                           IOConnectorProvider ioConnectorProvider) {
        Objects.requireNonNull(variantAnnotator);
        Objects.requireNonNull(dbAdaptor);
        this.dbAdaptor = dbAdaptor;
        this.variantAnnotator = variantAnnotator;
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
        if (overwrite) {
            query.remove(VariantQueryParam.ANNOTATION_EXISTS.key());
        } else {
            query.put(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
        }
        int checkpointSize = params.getInt(
                VariantStorageOptions.ANNOTATION_CHECKPOINT_SIZE.key(),
                VariantStorageOptions.ANNOTATION_CHECKPOINT_SIZE.defaultValue());

        preAnnotate(query, doCreate, doLoad, params);

        if (doCreate && doLoad) {
            ProjectMetadata.VariantAnnotatorProgram newAnnotator = variantAnnotator.getVariantAnnotatorProgram();
            List<ObjectMap> newSourceVersion = variantAnnotator.getVariantAnnotatorSourceVersion();

            dbAdaptor.getMetadataManager().updateProjectMetadata(projectMetadata -> {
                checkCurrentAnnotation(projectMetadata, overwrite, newAnnotator, newSourceVersion);
                return projectMetadata;
            });
        }

        long variants = 0;
        if (checkpointSize > 0 && doLoad && doCreate && doBatchAnnotation(params)) {
            int batch = 0;
            long newVariants;
            do {
                batch++;
                newVariants = annotateBatch(query, params, doCreate, doLoad, annotationFileStr, checkpointSize, batch, "." + batch);
            } while (newVariants == checkpointSize);
            variants += newVariants;
        } else {
            variants = annotateBatch(query, params, doCreate, doLoad, annotationFileStr, null, null, null);
        }
        logger.info("Finish annotation. New annotated variants: " + variants);

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
            Task<Variant, VariantAnnotation> annotationTask = variantList -> {
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
        } catch (ExecutionException e) {
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
        boolean annotateAll;
        Set<VariantQueryParam> queryParams = VariantQueryUtils.validParams(query, true);
        Set<String> filesFilter = Collections.emptySet();
        queryParams.removeAll(Arrays.asList(VariantQueryParam.ANNOTATION_EXISTS, VariantQueryParam.STUDY));

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
            List<Integer> studies = VariantQueryProjectionParser.getIncludeStudies(query, null, metadataManager);
            for (Integer studyId : studies) {
                List<Integer> files = new LinkedList<>();
                Collection<Integer> samples;
                List<Integer> annotatedFiles = new LinkedList<>();
                if (!filesFilter.isEmpty()) {
                    samples = new HashSet<>();
                    for (String file : filesFilter) {
                        FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, file);
                        if (fileMetadata != null) {
                            if (fileMetadata.isIndexed() && !fileMetadata.isAnnotated()) {
                                files.add(fileMetadata.getId());
                            }
                            for (Integer sample : fileMetadata.getSamples()) {
                                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sample);
                                if (sampleMetadata.isIndexed() && !sampleMetadata.isAnnotated()) {
                                    samples.add(sample);
                                }
                            }
                        }
                    }
                } else {
                    samples = new LinkedList<>();
                    metadataManager.fileMetadataIterator(studyId).forEachRemaining(fileMetadata -> {
                        if (fileMetadata.isIndexed()) {
                            if (fileMetadata.isAnnotated()) {
                                annotatedFiles.add(fileMetadata.getId());
                            } else {
                                files.add(fileMetadata.getId());
                            }
                        }
                    });
                    metadataManager.sampleMetadataIterator(studyId).forEachRemaining(sampleMetadata -> {
                        if (sampleMetadata.isIndexed()) {
                            if (!sampleMetadata.isAnnotated()) {
                                samples.add(sampleMetadata.getId());
                            }
                        }
                    });
                }
                filesToBeAnnotated.put(studyId, files);
                samplesToBeAnnotated.put(studyId, samples);
                alreadyAnnotatedFiles.put(studyId, annotatedFiles);
            }
            logger.info("Annotating {} new files and {} new samples",
                    filesToBeAnnotated.values().stream().mapToInt(Collection::size).sum(),
                    samplesToBeAnnotated.values().stream().mapToInt(Collection::size).sum()
            );
        }
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
        if (doLoad && doCreate) {
            ProjectMetadata.VariantAnnotatorProgram newAnnotator = variantAnnotator.getVariantAnnotatorProgram();
            List<ObjectMap> newSourceVersion = variantAnnotator.getVariantAnnotatorSourceVersion();


            dbAdaptor.getMetadataManager().updateProjectMetadata(projectMetadata -> {
                updateCurrentAnnotation(variantAnnotator, projectMetadata, overwrite, newAnnotator, newSourceVersion);
                return projectMetadata;
            });
        }

        if (doLoad && filesToBeAnnotated != null) {
            VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();

            for (Map.Entry<Integer, Collection<Integer>> entry : samplesToBeAnnotated.entrySet()) {
                Integer studyId = entry.getKey();
                Collection<Integer> sampleIds = entry.getValue();
                for (Integer sampleId : sampleIds) {
                    metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                        sampleMetadata.setAnnotationStatus(TaskMetadata.Status.READY);
                    });
                }
            }

            for (Map.Entry<Integer, List<Integer>> entry : filesToBeAnnotated.entrySet()) {
                Integer studyId = entry.getKey();
                List<Integer> fileIds = entry.getValue();

                for (Integer file : fileIds) {
                    metadataManager.updateFileMetadata(studyId, file, fileMetadata -> {
                        fileMetadata.setAnnotationStatus(TaskMetadata.Status.READY);
                    });
                }
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
                    variantReaderUtils.getVariantVcfReader(Paths.get(fileName), metadata),
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

    //TODO: Make this method abstract
    @Override
    public void deleteAnnotation(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException {
        throw new UnsupportedOperationException();
    }
}
