package org.opencb.opencga.storage.core.variant.annotation;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.TimeUtils;
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
 */
public class VariantAnnotationManager {

    public static final String CLEAN = "clean";
    public static final String FILE_NAME = "fileName";
    public static final String OUT_DIR = "outDir";
    public static final String ANNOTATOR_QUERY_OPTIONS = "annotatorQueryOptions";   // TODO use or remove
    public static final String BATCH_SIZE = "batchSize";
    public static final String NUM_WRITERS = "numWriters";

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

    public void annotate(QueryOptions options) throws IOException {

        long start = System.currentTimeMillis();
        logger.info("Starting annotation creation ");
        URI annotationFile = createAnnotation(
                Paths.get(options.getString(OUT_DIR, "/tmp")),
                options.getString(FILE_NAME, "annotation_" + TimeUtils.getTime()),
                options);
        logger.info("Finished annotation creation {}ms, generated file {}", System.currentTimeMillis() - start, annotationFile);

        start = System.currentTimeMillis();
        logger.info("Starting annotation creation ");
        loadAnnotation(annotationFile, options);
        logger.info("Finished annotation load {}ms", System.currentTimeMillis() - start);
    }

    public URI createAnnotation(Path outDir, String fileName, QueryOptions options) throws IOException {
        return this.variantAnnotator.createAnnotation(dbAdaptor, outDir, fileName, options);
    }

    public void loadAnnotation(URI uri, QueryOptions options) throws IOException {
        variantAnnotator.loadAnnotation(dbAdaptor, uri, options);
    }

    public enum AnnotationSource {
        CELLBASE_DB_ADAPTOR,
        CELLBASE_REST
    }

    public static VariantAnnotator buildVariantAnnotator(AnnotationSource source, Properties annotationProperties, String species, String assembly)
            throws VariantAnnotatorException {
        switch (source) {
            case CELLBASE_DB_ADAPTOR:
                return CellBaseVariantAnnotator.buildCellbaseAnnotator(annotationProperties, species, assembly, false);
            case CELLBASE_REST:
                return CellBaseVariantAnnotator.buildCellbaseAnnotator(annotationProperties, species, assembly, true);
            default:
                //TODO: Reflexion?
                throw new VariantAnnotatorException("Unknown annotation source: " + source);
        }
    }

    class AnnotatorDBReader implements DataReader<VariantAnnotation> {
        private final VariantDBIterator iterator;
        private final VariantDBAdaptor variantDBAdaptor;
        AnnotatorDBReader(VariantDBAdaptor variantDBAdaptor, QueryOptions options) {
            this.variantDBAdaptor = variantDBAdaptor;
            QueryOptions iteratorQueryOptions = new QueryOptions(options);
            iteratorQueryOptions.add("include", Arrays.asList("chromosome", "start", "end", "alternative", "reference"));
            this.iterator = variantDBAdaptor.iterator(iteratorQueryOptions);
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
                    batch.add(new VariantAnnotation(variant.getChromosome(), variant.getStart(), variant.getEnd(), variant.getReference(), variant.getAlternate()));
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
