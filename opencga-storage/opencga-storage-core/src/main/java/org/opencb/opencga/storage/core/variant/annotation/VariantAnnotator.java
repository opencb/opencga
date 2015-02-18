package org.opencb.opencga.storage.core.variant.annotation;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

/**
 * Created by jacobo on 9/01/15.
 */
public interface VariantAnnotator {

    /**
     * Creates a variant annotation file from an specific source based on the content of a Variant DataBase.
     *
     * @param variantDBAdaptor      DBAdaptor to the variant db
     * @param outDir                File outdir.
     * @param fileName              Generated file name.
     * @param options               Specific options.
     * @return                      URI of the generated file.
     * @throws IOException
     */
    URI createAnnotation(VariantDBAdaptor variantDBAdaptor, Path outDir, String fileName, QueryOptions options)
            throws IOException;

    /**
     * Loads variant annotations from an specified file into the selected Variant DataBase
     *
     * @param variantDBAdaptor      DBAdaptor to the variant db
     * @param uri                   URI of the annotation file
     * @param options               Specific options.
     * @throws IOException
     */
    void loadAnnotation(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException;

}
