package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.family.GenotypeCodec;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.QueryOperation;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.EMPTY_MASK;

/**
 * Created on 12/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexQuery {
    private static final byte[] EMPTY_INDEX_MASK = {0, 0};
    static final boolean[] EMPTY_PARENT_FILTER;

    static {
        EMPTY_PARENT_FILTER = new boolean[GenotypeCodec.NUM_CODES];
        // All true values
        for (int i = 0; i < EMPTY_PARENT_FILTER.length; i++) {
            EMPTY_PARENT_FILTER[i] = true;
        }
    }

    private final List<Region> regions;
    private final Set<VariantType> variantTypes;
    private final String study;
    private final Map<String, List<String>> samplesMap;
    private final Map<String, boolean[]> fatherFilter;
    private final Map<String, boolean[]> motherFilter;
    private final Map<String, byte[]> fileFilterMap; // byte[] = {mask , index}
    private final byte annotationIndexMask;
    private final Set<String> mendelianErrorSet;
    private final boolean onlyDeNovo;
    private final VariantQueryUtils.QueryOperation queryOperation;

    public SampleIndexQuery(List<Region> regions, String study, Map<String, List<String>> samplesMap, QueryOperation queryOperation) {
        this(regions, null, study, samplesMap, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), EMPTY_MASK,
                Collections.emptySet(), false, queryOperation);
    }

    public SampleIndexQuery(List<Region> regions, Set<VariantType> variantTypes, String study, Map<String, List<String>> samplesMap,
                            Map<String, boolean[]> fatherFilter, Map<String, boolean[]> motherFilter,
                            Map<String, byte[]> fileFilterMap, byte annotationIndexMask, Set<String> mendelianErrorSet,
                            boolean onlyDeNovo, QueryOperation queryOperation) {
        this.regions = regions;
        this.variantTypes = variantTypes;
        this.study = study;
        this.samplesMap = samplesMap;
        this.fatherFilter = fatherFilter;
        this.motherFilter = motherFilter;
        this.fileFilterMap = fileFilterMap;
        this.annotationIndexMask = annotationIndexMask;
        this.mendelianErrorSet = mendelianErrorSet;
        this.onlyDeNovo = onlyDeNovo;
        this.queryOperation = queryOperation;
    }

    public List<Region> getRegions() {
        return regions;
    }

    public Set<VariantType> getVariantTypes() {
        return variantTypes;
    }

    public String getStudy() {
        return study;
    }

    public Map<String, List<String>> getSamplesMap() {
        return samplesMap;
    }

    public Map<String, boolean[]> getFatherFilterMap() {
        return fatherFilter;
    }

    public boolean[] getFatherFilter(String sample) {
        return fatherFilter.getOrDefault(sample, EMPTY_PARENT_FILTER);
    }

    public Map<String, boolean[]> getMotherFilterMap() {
        return motherFilter;
    }

    public boolean[] getMotherFilter(String sample) {
        return motherFilter.getOrDefault(sample, EMPTY_PARENT_FILTER);
    }

    public byte getFileIndexMask(String sample) {
        return fileFilterMap.getOrDefault(sample, EMPTY_INDEX_MASK)[0];
    }

    public byte getFileIndex(String sample) {
        return fileFilterMap.getOrDefault(sample, EMPTY_INDEX_MASK)[1];
    }

    public boolean emptyFileIndex() {
        return fileFilterMap.isEmpty() || fileFilterMap.values().stream().allMatch(fileIndex -> fileIndex[0] == EMPTY_MASK);
    }

    public byte getAnnotationIndexMask() {
        return annotationIndexMask;
    }

    public boolean emptyAnnotationIndex() {
        return annotationIndexMask == EMPTY_MASK;
    }

    public VariantQueryUtils.QueryOperation getQueryOperation() {
        return queryOperation;
    }

    public boolean isOnlyDeNovo() {
        return onlyDeNovo;
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

    /**
     * Create a SingleSampleIndexQuery.
     *
     * @param sample Sample to query
     * @return SingleSampleIndexQuery
     */
    SingleSampleIndexQuery forSample(String sample) {
        return new SingleSampleIndexQuery(this, sample);
    }

    public static class SingleSampleIndexQuery extends SampleIndexQuery {

        private final String sample;
        private final List<String> gts;
        private final byte fileIndexMask;
        private final byte fileIndex;
        private final boolean[] fatherFilter;
        private final boolean[] motherFilter;
        private final boolean mendelianError;

        protected SingleSampleIndexQuery(SampleIndexQuery query, String sample) {
            this(query, sample, query.getSamplesMap().get(sample));
        }

        protected SingleSampleIndexQuery(SampleIndexQuery query, String sample, List<String> gts) {
            super(query.regions == null ? null : new ArrayList<>(query.regions),
                    query.variantTypes == null ? null : new HashSet<>(query.variantTypes),
                    query.study,
                    Collections.singletonMap(sample, gts),
                    query.fatherFilter,
                    query.motherFilter,
                    query.fileFilterMap,
                    query.annotationIndexMask,
                    query.mendelianErrorSet,
                    query.onlyDeNovo,
                    query.queryOperation);
            this.sample = sample;
            this.gts = gts;
            fatherFilter = getFatherFilter(sample);
            motherFilter = getMotherFilter(sample);
            fileIndexMask = getFileIndexMask(sample);
            fileIndex = getFileIndex(sample);
            mendelianError = query.mendelianErrorSet.contains(sample);
        }

        @Override
        public boolean emptyFileIndex() {
            return fileIndexMask == EMPTY_MASK;
        }

        public String getSample() {
            return sample;
        }

        public List<String> getGenotypes() {
            return gts;
        }

        public boolean[] getFatherFilter() {
            return fatherFilter;
        }

        public boolean hasFatherFilter() {
            return fatherFilter != EMPTY_PARENT_FILTER;
        }

        public boolean[] getMotherFilter() {
            return motherFilter;
        }

        public boolean hasMotherFilter() {
            return motherFilter != EMPTY_PARENT_FILTER;
        }

        public byte getFileIndexMask() {
            return fileIndexMask;
        }

        public byte getFileIndex() {
            return fileIndex;
        }

        public boolean getMendelianError() {
            return mendelianError;
        }
    }
}
