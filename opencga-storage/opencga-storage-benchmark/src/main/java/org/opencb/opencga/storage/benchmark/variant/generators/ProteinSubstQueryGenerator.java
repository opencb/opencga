package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jtarraga on 10/04/17.
 */
public class ProteinSubstQueryGenerator extends ScoreQueryGenerator {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public ProteinSubstQueryGenerator(List<Score> scores, List<String> ops, String queryKey) {
        super(null, Arrays.asList(">,>=,<,<=".split(",")), VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_SUBSTITUTION.key());

        scores = new ArrayList<>();
        scores.add(new Score("sift", 0, 1));
        scores.add(new Score("polyphen", 0, 1));
    }
}
