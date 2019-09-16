package org.opencb.opencga.storage.core.variant.score;

import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantScore;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VariantScoreParser implements Task<String, Pair<Variant, VariantScore>> {
    private final int variantColumnIdx;
    private final int chrColumnIdx;
    private final int posColumnIdx;
    private final int refColumnIdx;
    private final int altColumnIdx;
    private final int scoreColumnIdx;
    private final int pValueColumnIdx;
    private final VariantScoreMetadata scoreMetadata;
    private final String cohortName;
    private final String secondCohortName;
    private final String separator;
    private final VariantNormalizer variantNormalizer;

    public VariantScoreParser(VariantScoreFormatDescriptor descriptor, VariantScoreMetadata scoreMetadata,
                              String cohortName, String secondCohortName) {
        this.variantColumnIdx = descriptor.getVariantColumnIdx();
        this.chrColumnIdx = descriptor.getChrColumnIdx();
        this.posColumnIdx = descriptor.getPosColumnIdx();
        this.refColumnIdx = descriptor.getRefColumnIdx();
        this.altColumnIdx = descriptor.getAltColumnIdx();
        this.scoreColumnIdx = descriptor.getScoreColumnIdx();
        this.pValueColumnIdx = descriptor.getPvalueColumnIdx();
        this.scoreMetadata = scoreMetadata;
        this.cohortName = cohortName;
        this.secondCohortName = secondCohortName;
        this.separator = "\t";
        variantNormalizer = new VariantNormalizer();
    }


    @Override
    public List<Pair<Variant, VariantScore>> apply(List<String> list) throws Exception {
        List<Pair<Variant, VariantScore>> variantScores = new ArrayList<>(list.size());
        for (String line : list) {
            if (line.startsWith("#")) {
                continue;
            }
            String[] split = line.split(separator);
            Variant variant;
            variant = variantColumnIdx >= 0
                    ? new Variant(split[variantColumnIdx])
                    : new Variant(
                    split[chrColumnIdx],
                    Integer.parseInt(split[posColumnIdx]),
                    split[refColumnIdx],
                    split[altColumnIdx]);
            variant = variantNormalizer.normalize(Collections.singletonList(variant), true).get(0);


            Float score = parseFloat(split, scoreColumnIdx);
            Float pvalue = parseFloat(split, pValueColumnIdx);
            VariantScore variantScore = new VariantScore(scoreMetadata.getName(), cohortName, secondCohortName, score, pvalue);

            variantScores.add(Pair.of(variant, variantScore));
        }
        return variantScores;
    }

    private Float parseFloat(String[] split, int columnIdx) {
        if (columnIdx < 0) {
            return Float.NaN;
        }
        String str = split[columnIdx];
        if (str.equals(".") || str.equals("NA")) {
            return Float.NaN;
        }
        return Float.valueOf(str);
    }
}
