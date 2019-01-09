package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.QueryOperation;

/**
 * Created on 12/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class SampleIndexQuery {
    private final List<Region> regions;
    private final String study;
    private final Map<String, List<String>> samplesMap;
    private final byte annotationMask;
    private final VariantQueryUtils.QueryOperation queryOperation;

    public SampleIndexQuery(List<Region> regions, String study,
                             Map<String, List<String>> samplesMap,
                             byte annotationMask, QueryOperation queryOperation) {
        this.regions = regions;
        this.study = study;
        this.samplesMap = samplesMap;
        this.annotationMask = annotationMask;
        this.queryOperation = queryOperation;
    }

    public List<Region> getRegions() {
        return regions;
    }

    public String getStudy() {
        return study;
    }

    public Map<String, List<String>> getSamplesMap() {
        return samplesMap;
    }

    public byte getAnnotationMask() {
        return annotationMask;
    }

    public VariantQueryUtils.QueryOperation getQueryOperation() {
        return queryOperation;
    }
}
