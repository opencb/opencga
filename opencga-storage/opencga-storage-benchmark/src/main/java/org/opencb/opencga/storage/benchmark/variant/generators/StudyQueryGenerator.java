package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 07/04/17.
 *
 * @author Joaquín Tárraga &lt;joaquintarraga@gmail.com&gt;
 */
public class StudyQueryGenerator extends TermQueryGenerator {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public StudyQueryGenerator() {
        super("studies.csv", VariantDBAdaptor.VariantQueryParams.STUDIES.key());
    }
}
