package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions;

import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationExtensionConfigureParams;

import java.net.URI;
import java.util.List;

public interface VariantAnnotatorExtensionTask extends Task<VariantAnnotation, VariantAnnotation> {

    String NAME_KEY = "name";
    String INDEX_CREATION_DATE_KEY = "indexCreationDate";

    /**
     * Get the variant annotator extension task ID.
     *
     * @return Variant annotator extension task ID
     */
    String getId();

    /**
     * Set up the variant annotator extension.
     * This method will be called before any other method. It might generate extra files or data needed for the annotation.
     *
     * @param configureParams Parameters to configure the variant annotation extension
     * @param outDir  Output directory where the annotator extension should write the files
     * @return ObjectMap with the configuration options of the annotator extension
     * @throws Exception if the annotator extension set up fails
     */
    List<URI> setup(VariantAnnotationExtensionConfigureParams configureParams, URI outDir) throws Exception;

    /**
     * Check extension parameters and if they are compatibly with the options.
     * @throws IllegalArgumentException if extension parameters are missing or incompatible with the options
     */
    void check(ObjectMap options) throws IllegalArgumentException;

    /**
     * Check if the annotator extension is available for the given options.
     * @throws IllegalArgumentException if the annotator extension is not available
     */
    void checkAvailable() throws IllegalArgumentException;

    /**
     * Check if the annotator extension is available for the given options. Do not throw any exception if the extension is not available.
     * @return true if the annotator extension is available
     */
    default boolean isAvailable() {
        try {
            checkAvailable();
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    default void pre() throws Exception {
        Task.super.pre();
        checkAvailable();
    }

    /**
     * Get the options for the annotator extension.
     * @return Options for the annotator extension
     */
    ObjectMap getOptions();

    /**
     * Get the metadata for the annotator extension.
     * @return Metadata for the annotator extension
     */
    ObjectMap getMetadata();

}
