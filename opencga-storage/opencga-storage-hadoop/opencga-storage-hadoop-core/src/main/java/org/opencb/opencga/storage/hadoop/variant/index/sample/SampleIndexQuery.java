package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.QueryOperation;

/**
 * Created on 12/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexQuery {
    private final List<Region> regions;
    private final String study;
    private final Map<String, List<String>> samplesMap;
    private final byte fileIndexMask;
    private final byte fileIndex;
    private final byte annotationIndexMask;
    private final VariantQueryUtils.QueryOperation queryOperation;

    public SampleIndexQuery(List<Region> regions, String study,
                            Map<String, List<String>> samplesMap, byte fileIndexMask, byte fileIndex,
                            byte annotationIndexMask, QueryOperation queryOperation) {
        this.regions = regions;
        this.study = study;
        this.samplesMap = samplesMap;
        this.fileIndexMask = fileIndexMask;
        this.fileIndex = fileIndex;
        this.annotationIndexMask = annotationIndexMask;
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

    public byte getFileIndexMask() {
        return fileIndexMask;
    }

    public byte getFileIndex() {
        return fileIndex;
    }

    public byte getAnnotationIndexMask() {
        return annotationIndexMask;
    }

    public VariantQueryUtils.QueryOperation getQueryOperation() {
        return queryOperation;
    }

    /**
     * Create a SingleSampleIndexQuery.
     *
     * @param sample Sample to query
     * @param gts    Processed list of GTs. Real GTs only.
     * @return SingleSampleIndexQuery
     */
    SingleSampleIndexQuery forSample(String sample, List<String> gts) {
        return new SingleSampleIndexQuery(this, sample, gts);
    }

    public static class SingleSampleIndexQuery extends SampleIndexQuery {

        private final String sample;
        private final List<String> gts;

        protected SingleSampleIndexQuery(SampleIndexQuery query, String sample) {
            this(query, sample, query.getSamplesMap().get(sample));
        }

        protected SingleSampleIndexQuery(SampleIndexQuery query, String sample, List<String> gts) {
            super(query.regions == null ? null : new ArrayList<>(query.regions),
                    query.study,
                    Collections.singletonMap(sample, gts),
                    query.fileIndexMask,
                    query.fileIndex,
                    query.annotationIndexMask,
                    query.queryOperation);
            this.sample = sample;
            this.gts = gts;
        }

        public String getSample() {
            return sample;
        }

        public List<String> getGenotypes() {
            return gts;
        }
    }
}
