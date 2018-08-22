package org.opencb.opencga.storage.mongodb.variant.converters.trash;

import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

/**
 * Created on 27/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DocumentToTrashVariantConverter extends DocumentToVariantConverter {
    public DocumentToTrashVariantConverter() {
    }

    public static final String TIMESTAMP_FIELD = "ts";
}
