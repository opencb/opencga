package org.opencb.opencga.storage.core.variant.annotation;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.IOException;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantAnnotationManager {

    String SPECIES = "species";
    String ASSEMBLY = "assembly";
    String ANNOTATION_SOURCE = "annotationSource";
    String OVERWRITE_ANNOTATIONS = "overwriteAnnotations";
    String VARIANT_ANNOTATOR_CLASSNAME = "variant.annotator.classname";
    // File to load.
    String CREATE = "annotation.create";
    String LOAD_FILE = "annotation.load.file";
    String CUSTOM_ANNOTATION_KEY = "custom_annotation_key";

    void annotate(Query query, QueryOptions options) throws VariantAnnotatorException, IOException, StorageManagerException;

}
