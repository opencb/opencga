package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by jtarraga on 10/04/17.
 */
public class FunctionalScoreQueryGenerator extends ScoreQueryGenerator {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public FunctionalScoreQueryGenerator() {
        super(null, Arrays.asList(">,>=,<,<=".split(",")), VariantDBAdaptor.VariantQueryParams.ANNOT_FUNCTIONAL_SCORE.key());

        scores = new ArrayList<>();
        scores.add(new Score("caddRaw", 0, 1));
        scores.add(new Score("caddScaled", -10, 40));
    }
}
