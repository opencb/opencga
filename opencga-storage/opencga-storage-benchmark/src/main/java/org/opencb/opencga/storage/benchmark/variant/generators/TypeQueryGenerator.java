package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 07/04/17.
 *
 * @author Joaquín Tárraga &lt;joaquintarraga@gmail.com&gt;
 */
public class TypeQueryGenerator extends TermQueryGenerator {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public TypeQueryGenerator() {
        super("types.csv", VariantDBAdaptor.VariantQueryParams.TYPE.key());
    }
}
