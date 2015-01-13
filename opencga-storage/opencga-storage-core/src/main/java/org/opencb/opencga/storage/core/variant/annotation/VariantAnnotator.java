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

    URI createAnnotation(VariantDBAdaptor variantDBAdaptor, Path outDir, String fileName, QueryOptions options) throws IOException;

    void loadAnnotation(VariantDBAdaptor variantDBAdaptor, URI uri, boolean clean);

}
