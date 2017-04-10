package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created on 07/04/17.
 *
 * @author Joaquín Tárraga &lt;joaquintarraga@gmail.com&gt;
 */
public class ConservationQueryGenerator extends ScoreQueryGenerator {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public ConservationQueryGenerator() {
        super(null, Arrays.asList(">,>=,<,<=".split(",")), VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key());

        scores = new ArrayList<>();
        scores.add(new Score("phylop", 0, 1));
        scores.add(new Score("phastcons", 0, 1));
        scores.add(new Score("gerp", 0, 1));
    }
}
