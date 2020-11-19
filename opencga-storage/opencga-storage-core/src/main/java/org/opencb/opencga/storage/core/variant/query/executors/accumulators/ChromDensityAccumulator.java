package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class ChromDensityAccumulator<T> extends FacetFieldAccumulator<T> {
    private final Region region;
    private final int step;
    private final int numSteps;
    private final Function<T, Integer> getStart;

    public ChromDensityAccumulator(VariantStorageMetadataManager metadataManager, Region region,
                                   FacetFieldAccumulator<T> nestedFieldAccumulator, int step, Function<T, Integer> getStart) {
        super(nestedFieldAccumulator);
        this.region = region;
        this.step = step;
        this.getStart = getStart;

        if (region.getEnd() == Integer.MAX_VALUE) {
            for (Integer studyId : metadataManager.getStudyIds()) {
                StudyMetadata studyMetadata = metadataManager.getStudyMetadata(studyId);
                VariantFileHeaderComplexLine contig = studyMetadata.getVariantHeaderLine("contig", region.getChromosome());
                if (contig == null) {
                    contig = studyMetadata.getVariantHeaderLine("contig", "chr" + region.getChromosome());
                }
                if (contig != null) {
                    String length = contig.getGenericFields().get("length");
                    if (StringUtils.isNotEmpty(length) && StringUtils.isNumeric(length)) {
                        region.setEnd(Integer.parseInt(length));
                        break;
                    }
                }
            }
        }
        if (region.getStart() == 0) {
            region.setStart(1);
        }

        int regionLength = region.getEnd() - region.getStart();
        if (regionLength != Integer.MAX_VALUE) {
            regionLength++;
        }
        numSteps = regionLength / step + 1;
    }

    @Override
    public String getName() {
        return VariantField.START.fieldName();
    }

    @Override
    public FacetField createField() {
        return new FacetField(VariantField.START.fieldName(), 0,
                prepareBuckets())
                .setStart(region.getStart())
                .setEnd(region.getEnd())
                .setStep(step);
    }

    @Override
    public List<FacetField.Bucket> prepareBuckets1() {
        List<FacetField.Bucket> valueBuckets = new ArrayList<>(numSteps);
        for (int i = 0; i < numSteps; i++) {
            valueBuckets.add(new FacetField.Bucket(String.valueOf(i * step + region.getStart()), 0, null));
        }
        return valueBuckets;
    }

    @Override
    protected List<FacetField.Bucket> getBuckets(FacetField field, T t) {
        int idx = (getStart(t) - region.getStart()) / step;
        if (idx < numSteps) {
            return Collections.singletonList(field.getBuckets().get(idx));
        } else {
            return null;
        }
    }

    protected Integer getStart(T variant) {
        return getStart.apply(variant);
    }
}
