/*
 * Copyright 2015 OpenCB
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

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantDBReader;
import org.opencb.opencga.storage.core.variant.io.avro.AvroDataReader;
import org.opencb.opencga.storage.core.variant.io.avro.AvroDataWriter;
import org.opencb.opencga.storage.core.variant.io.avro.VariantAnnotationJsonDataReader;
import org.opencb.opencga.storage.core.variant.io.avro.VariantAnnotationJsonDataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by jacobo on 9/01/15.
 *
 * @author Javier Lopez &lt;fjlopez@ebi.ac.uk&gt;
 */
public class VariantAnnotationManager {

    public static final String SPECIES = "species";
    public static final String ASSEMBLY = "assembly";
    public static final String ANNOTATION_SOURCE = "annotationSource";
    //    public static final String ANNOTATOR_PROPERTIES = "annotatorProperties";
    public static final String OVERWRITE_ANNOTATIONS = "overwriteAnnotations";

    public static final String CLEAN = "clean";
    public static final String FILE_NAME = "fileName";
    public static final String OUT_DIR = "outDir";
    public static final String ANNOTATOR_QUERY_OPTIONS = "annotatorQueryOptions";   // TODO use or remove
    public static final String BATCH_SIZE = "batchSize";
    public static final String NUM_WRITERS = "numWriters";
    public static final String NUM_THREADS = "numThreads";
    public static final String VARIANT_ANNOTATOR_CLASSNAME = "variant.annotator.classname";

    private VariantDBAdaptor dbAdaptor;
    private VariantAnnotator variantAnnotator;
    protected static Logger logger = LoggerFactory.getLogger(VariantAnnotationManager.class);

    public VariantAnnotationManager(VariantAnnotator variantAnnotator, VariantDBAdaptor dbAdaptor) {
        if (dbAdaptor == null || variantAnnotator == null) {
            throw new NullPointerException();
        }
        this.dbAdaptor = dbAdaptor;
        this.variantAnnotator = variantAnnotator;
    }

    public void annotate(Query query, QueryOptions options) throws IOException {

        long start = System.currentTimeMillis();
        logger.info("Starting annotation creation ");
        URI annotationFile = createAnnotation(
                Paths.get(options.getString(OUT_DIR, "/tmp")),
                options.getString(FILE_NAME, "annotation_" + TimeUtils.getTime()),
                query, options);
        logger.info("Finished annotation creation {}ms, generated file {}", System.currentTimeMillis() - start, annotationFile);

        start = System.currentTimeMillis();
        logger.info("Starting annotation creation ");
        loadAnnotation(annotationFile, options);
        logger.info("Finished annotation load {}ms", System.currentTimeMillis() - start);
    }

    /**
     * Creates a variant annotation file from an specific source based on the content of a Variant DataBase.
     *
     * @param outDir   File outdir.
     * @param fileName Generated file name.
     * @param query    Query for those variants to annotate.
     * @param options  Specific options.
     * @return URI of the generated file.
     * @throws IOException IOException thrown
     */
    public URI createAnnotation(Path outDir, String fileName, Query query, QueryOptions options) throws IOException {

        boolean gzip = options == null || options.getBoolean("gzip", true);
        boolean avro = options == null || options.getBoolean("avro", false);
        Path path = Paths.get(outDir != null
                ? outDir.toString()
                : "/tmp", fileName + ".annot" + (avro ? ".avro" : ".json") + (gzip ? ".gz" : ""));
        URI fileUri = path.toUri();

        /** Getting iterator from OpenCGA Variant database. **/
        QueryOptions iteratorQueryOptions;
        if (options == null) {
            iteratorQueryOptions = new QueryOptions();
        } else {
            iteratorQueryOptions = new QueryOptions(options);
        }
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternate", "reference");
        iteratorQueryOptions.add("include", include);

        int batchSize = 200;
        int numThreads = 8;
        if (options != null) { //Parse query options
            batchSize = options.getInt(VariantAnnotationManager.BATCH_SIZE, batchSize);
            numThreads = options.getInt(VariantAnnotationManager.NUM_THREADS, numThreads);
        }

        try {
            DataReader<Variant> variantDataReader = new VariantDBReader(dbAdaptor, query, iteratorQueryOptions);

            ParallelTaskRunner.Task<Variant, VariantAnnotation> annotationTask = variantList -> {
                List<VariantAnnotation> variantAnnotationList;
                long start = System.currentTimeMillis();
                logger.debug("Annotating batch of {} genomic variants.", variantList.size());
                try {
                    variantAnnotationList = variantAnnotator.annotate(variantList);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
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

            ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numThreads, batchSize, numThreads * 2, true, false);
            ParallelTaskRunner<Variant, VariantAnnotation> parallelTaskRunner =
                    new ParallelTaskRunner<>(variantDataReader, annotationTask, variantAnnotationDataWriter, config);
            parallelTaskRunner.run();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }

        return fileUri;
    }

    /**
     * Loads variant annotations from an specified file into the selected Variant DataBase.
     *
     * @param uri     URI of the annotation file
     * @param options Specific options.
     * @throws IOException IOException thrown
     */
    public void loadAnnotation(URI uri, QueryOptions options) throws IOException {

        final int batchSize = options.getInt(VariantAnnotationManager.BATCH_SIZE, 100);
        final int numConsumers = options.getInt(VariantAnnotationManager.NUM_WRITERS, 6);
        boolean avro = uri.getPath().endsWith("avro") || uri.getPath().endsWith("avro.gz");

        dbAdaptor.preUpdateAnnotations();

        ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numConsumers, batchSize, numConsumers * 2, true, false);
        DataReader<VariantAnnotation> reader;

        //TODO: Read from VEP file
        if (avro) {
            reader = new AvroDataReader<>(Paths.get(uri).toFile(), VariantAnnotation.class);
        } else {
            reader = new VariantAnnotationJsonDataReader(Paths.get(uri).toFile());
        }
        try {
            ParallelTaskRunner<VariantAnnotation, Void> prt = new ParallelTaskRunner<>(reader, variantAnnotationList -> {
                dbAdaptor.updateAnnotations(variantAnnotationList, new QueryOptions());
                return Collections.emptyList();
            }, null, config);
            prt.run();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public enum AnnotationSource {
        CELLBASE_DB_ADAPTOR,
        CELLBASE_REST,
        VEP,
        OTHER
    }


    public static VariantAnnotator buildVariantAnnotator(StorageConfiguration configuration, String storageEngineId)
            throws VariantAnnotatorException {
        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();
        AnnotationSource annotationSource = VariantAnnotationManager.AnnotationSource.valueOf(
                options.getString(ANNOTATION_SOURCE, options.containsKey(VARIANT_ANNOTATOR_CLASSNAME)
                        ? AnnotationSource.OTHER.name()
                        : VariantAnnotationManager.AnnotationSource.CELLBASE_REST.name()).toUpperCase()
        );

        logger.info("Annotating with {}", annotationSource);

        switch (annotationSource) {
            case CELLBASE_DB_ADAPTOR:
                return new CellBaseVariantAnnotator(configuration, options, false);
            case CELLBASE_REST:
                return new CellBaseVariantAnnotator(configuration, options, true);
            case VEP:
                return VepVariantAnnotator.buildVepAnnotator();
            case OTHER:
            default:
                String className = options.getString(VARIANT_ANNOTATOR_CLASSNAME);
                try {
                    Class<?> clazz = Class.forName(className);
                    if (VariantAnnotator.class.isAssignableFrom(clazz)) {
                        return (VariantAnnotator) clazz.getConstructor(StorageConfiguration.class, ObjectMap.class)
                                .newInstance(configuration, options);
                    } else {
                        throw new VariantAnnotatorException("Invalid VariantAnnotator class: " + className);
                    }
                } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                    throw new VariantAnnotatorException("Unable to create annotation source from \"" + className + "\"", e);
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                    throw new VariantAnnotatorException("Unable to create annotation source from \"" + className + "\"", e);
                }
        }

    }

}
