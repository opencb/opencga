package org.opencb.opencga.storage.core.variant.annotation;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.TimeUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

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

}
