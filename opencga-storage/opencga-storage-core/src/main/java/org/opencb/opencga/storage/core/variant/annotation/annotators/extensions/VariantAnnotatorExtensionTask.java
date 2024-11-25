package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions;

import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.Task;

import java.net.URI;
import java.util.List;

public interface VariantAnnotatorExtensionTask extends Task<VariantAnnotation, VariantAnnotation> {

    /**
     * Set up the annotator extension.
     * This method will be called before any other method. It might generate extra files or data needed for the annotation.
     *
     * @param output  Output directory where the annotator extension should write the files
     * @return List of URIs of generated files (if any)
     * @throws Exception if the annotator extension set up fails
     */
    List<URI> setup(URI output) throws Exception;

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
