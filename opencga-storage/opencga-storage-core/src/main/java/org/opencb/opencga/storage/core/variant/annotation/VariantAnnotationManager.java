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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by jacobo on 9/01/15.
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

    private VariantDBAdaptor dbAdaptor;
    private VariantAnnotator variantAnnotator;
    protected static Logger logger = LoggerFactory.getLogger(VariantAnnotationManager.class);

    public VariantAnnotationManager(VariantAnnotator variantAnnotator, VariantDBAdaptor dbAdaptor) {
        if(dbAdaptor == null || variantAnnotator == null) {
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

    public URI createAnnotation(Path outDir, String fileName, Query query, QueryOptions options) throws IOException {
        return this.variantAnnotator.createAnnotation(dbAdaptor, outDir, fileName, query, options);
    }

    public void loadAnnotation(URI uri, QueryOptions options) throws IOException {
        variantAnnotator.loadAnnotation(dbAdaptor, uri, options);
    }

    public enum AnnotationSource {
        CELLBASE_DB_ADAPTOR,
        CELLBASE_REST,
        VEP
    }


    public static VariantAnnotator buildVariantAnnotator(StorageConfiguration configuration, String storageEngineId)
            throws VariantAnnotatorException {
        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();
        AnnotationSource annotationSource = VariantAnnotationManager.AnnotationSource.valueOf(
                options.getString(ANNOTATION_SOURCE, VariantAnnotationManager.AnnotationSource.CELLBASE_REST.name()).toUpperCase()
        );

        logger.info("Annotating with {}", annotationSource);

        String species = options.getString(SPECIES);
        String assembly = options.getString(ASSEMBLY);

        switch (annotationSource) {
            case CELLBASE_DB_ADAPTOR:
                return CellBaseVariantAnnotator.buildCellbaseAnnotator(configuration.getCellbase(), species, assembly, false);
            case CELLBASE_REST:
                return CellBaseVariantAnnotator.buildCellbaseAnnotator(configuration.getCellbase(), species, assembly, true);
            case VEP:
                return VepVariantAnnotator.buildVepAnnotator();
            default:
                //TODO: Reflexion?
                throw new VariantAnnotatorException("Unknown annotation source: " + annotationSource);
        }

    }

    class AnnotatorDBReader implements DataReader<VariantAnnotation> {
        private final VariantDBIterator iterator;
        private final VariantDBAdaptor variantDBAdaptor;
        AnnotatorDBReader(VariantDBAdaptor variantDBAdaptor, Query query) {
            this.variantDBAdaptor = variantDBAdaptor;
            QueryOptions iteratorQueryOptions = new QueryOptions();
            iteratorQueryOptions.add("include", Arrays.asList("chromosome", "start", "end", "alternate", "reference"));
            this.iterator = variantDBAdaptor.iterator(query, iteratorQueryOptions);
        }
        @Override public boolean open() {return true;}
        @Override public boolean close() {return true;}
        @Override public boolean pre() {return true;}
        @Override public boolean post() {return true;}
        @Override public List<VariantAnnotation> read() { return read(1); }
        @Override public List<VariantAnnotation> read(int batchSize) {
            List<VariantAnnotation> batch = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
                Variant variant = iterator.next();
                // If Variant is SV some work is needed
                if(variant.getAlternate().length() + variant.getReference().length() > Variant.SV_THRESHOLD*2) {       //TODO: Manage SV variants
//                logger.info("Skip variant! {}", genomicVariant);
                    logger.info("Skip variant! {}", variant.getChromosome() + ":" +
                                    variant.getStart() + ":" +
                                    (variant.getReference().length() > 10? variant.getReference().substring(0,10) + "...[" + variant.getReference().length() + "]" : variant.getReference()) + ":" +
                                    (variant.getAlternate().length() > 10? variant.getAlternate().substring(0,10) + "...[" + variant.getAlternate().length() + "]" : variant.getAlternate())
                    );
                    logger.debug("Skip variant! {}", variant);
                } else {
//                    GenomicVariant genomicVariant = new GenomicVariant(variant.getChromosome(), variant.getStart(),
//                            variant.getReference().isEmpty() && variant.getType() == Variant.VariantType.INDEL ? "-" : variant.getReference(),
//                            variant.getAlternate().isEmpty() && variant.getType() == Variant.VariantType.INDEL ? "-" : variant.getAlternate());
//                    genomicVariantList.add(genomicVariant);
                    VariantAnnotation variantAnnotation = new VariantAnnotation();
                    variantAnnotation.setChromosome(variant.getChromosome());
                    variantAnnotation.setStart( variant.getStart());
                    variantAnnotation.setEnd( variant.getEnd());
                    variantAnnotation.setReference( variant.getReference());
                    variantAnnotation.setAlternate( variant.getAlternate());
                    batch.add(variantAnnotation);
                }
            }
            return batch;
        }
    }

    class AnnotatorDBWriter implements DataWriter<VariantAnnotation> {
        private final VariantDBAdaptor variantDBAdaptor;
        AnnotatorDBWriter(VariantDBAdaptor variantDBAdaptor) {
            this.variantDBAdaptor = variantDBAdaptor;
        }
        @Override public boolean open() {return true;}
        @Override public boolean close() {return true;}
        @Override public boolean pre() {return true;}
        @Override public boolean post() {return true;}
        @Override public boolean write(VariantAnnotation elem) {
            variantDBAdaptor.updateAnnotations(Collections.singletonList(elem), null);
            return true;
        }
        @Override public boolean write(List<VariantAnnotation> batch) {
            variantDBAdaptor.updateAnnotations(batch, null);
            return true;
        }
    }

    class AnnotatorJsonReader implements DataReader<VariantAnnotation> {
        private final JsonParser parser;
        private long readsCounter;
        AnnotatorJsonReader(InputStream inputStream) throws IOException {
            JsonFactory factory = new JsonFactory();
            ObjectMapper jsonObjectMapper = new ObjectMapper(factory);
            /** Innitialice Json parse**/
            parser = factory.createParser(inputStream);
        }

        @Override public boolean open() {return true;}
        @Override public boolean close() {return true;}
        @Override public boolean pre() {readsCounter = 0; return true;}
        @Override public boolean post() {return true;}
        @Override public List<VariantAnnotation> read() { return read(1); }
        @Override public List<VariantAnnotation> read(int batchSize) {
            List<VariantAnnotation> batch = new ArrayList<>(batchSize);
            try {
                for (int i = 0; i < batchSize && parser.nextToken() != null; i++) {
                    VariantAnnotation variantAnnotation = parser.readValueAs(VariantAnnotation.class);
                    batch.add(variantAnnotation);
                    readsCounter++;
                    if (readsCounter % 1000 == 0) {
                        logger.info("Element {}", readsCounter);
                    }
                }
            } catch (IOException e) {
                return Collections.emptyList();
            }
            return batch;
        }
    }

    class AnnotatorJsonWriter implements DataWriter<VariantAnnotation> {
        private final ObjectWriter writer;
        private final OutputStream outputStream;

        AnnotatorJsonWriter(OutputStream outputStream) {
            this.outputStream = outputStream;
            JsonFactory factory = new JsonFactory();
            ObjectMapper jsonObjectMapper = new ObjectMapper(factory);
            this.writer = jsonObjectMapper.writerWithType(VariantAnnotation.class);
        }
        @Override public boolean open() {return true;}
        @Override public boolean close() {return true;}
        @Override public boolean pre() {return true;}
        @Override public boolean post() {return true;}
        @Override  public boolean write(VariantAnnotation variantAnnotation) {
            return write(Collections.singletonList(variantAnnotation));
        }
        @Override  public boolean write(List<VariantAnnotation> batch) {
            for (VariantAnnotation variantAnnotation : batch) {
                try {
                    outputStream.write(writer.writeValueAsString(variantAnnotation).getBytes());
                    outputStream.write('\n');
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    return false;
                }
            }
            return true;
        }
    }

}
