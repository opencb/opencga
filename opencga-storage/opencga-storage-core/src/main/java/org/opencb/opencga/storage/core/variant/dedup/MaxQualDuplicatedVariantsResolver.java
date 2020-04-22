package org.opencb.opencga.storage.core.variant.dedup;

import htsjdk.variant.vcf.VCFConstants;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.io.managers.IOConnector;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class MaxQualDuplicatedVariantsResolver extends AbstractDuplicatedVariantsResolver {

    public MaxQualDuplicatedVariantsResolver(String variantFile, IOConnector ioConnector, URI duplicatedVariantsOutputFile) {
        super(variantFile, ioConnector, duplicatedVariantsOutputFile);
    }

    public static final Comparator<Variant> COMPARATOR = ((Comparator<Variant>) (v1, v2) -> {
        String f1 = v1.getStudies().get(0).getFile(0).getData().get(StudyEntry.FILTER);
        String f2 = v2.getStudies().get(0).getFile(0).getData().get(StudyEntry.FILTER);
        if (Objects.equals(f1, f2)) {
            return 0;
        }
        if (VCFConstants.PASSES_FILTERS_v4.equals(f1)) {
            return 1;
        }
        if (VCFConstants.PASSES_FILTERS_v4.equals(f2)) {
            return -1;
        }
        return 0;
    }).thenComparingDouble(v -> {
        String qual = v.getStudies().get(0).getFile(0).getData().get(StudyEntry.QUAL);
        if (qual == null || qual.isEmpty() || qual.equals(VCFConstants.MISSING_VALUE_v4)) {
            return 0;
        } else {
            try {
                return Double.parseDouble(qual);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
    }).reversed();


    @Override
    Variant internalResolveDuplicatedVariants(List<Variant> variants) {
        variants.sort(COMPARATOR);
        return variants.get(0);
    }
}
