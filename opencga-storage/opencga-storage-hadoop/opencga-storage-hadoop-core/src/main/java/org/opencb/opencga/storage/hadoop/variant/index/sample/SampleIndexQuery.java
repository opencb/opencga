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
    private final SampleAnnotationIndexQuery annotationIndexQuery;
    private final Set<String> mendelianErrorSet;
    private final boolean onlyDeNovo;
    private final VariantQueryUtils.QueryOperation queryOperation;

    public SampleIndexQuery(List<Region> regions, String study, Map<String, List<String>> samplesMap, QueryOperation queryOperation) {
        this(regions, null, study, samplesMap, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                new SampleAnnotationIndexQuery(), Collections.emptySet(), false, queryOperation);
    }

    public SampleIndexQuery(List<Region> regions, Set<VariantType> variantTypes, String study, Map<String, List<String>> samplesMap,
                            Map<String, boolean[]> fatherFilter, Map<String, boolean[]> motherFilter,
                            Map<String, byte[]> fileFilterMap,
                            SampleAnnotationIndexQuery annotationIndexQuery,
                            Set<String> mendelianErrorSet, boolean onlyDeNovo, QueryOperation queryOperation) {
        this.regions = regions;
        this.variantTypes = variantTypes;
        this.study = study;
        this.samplesMap = samplesMap;
        this.fatherFilter = fatherFilter;
        this.motherFilter = motherFilter;
        this.fileFilterMap = fileFilterMap;
        this.annotationIndexQuery = annotationIndexQuery;
        this.mendelianErrorSet = mendelianErrorSet;
        this.onlyDeNovo = onlyDeNovo;
        this.queryOperation = queryOperation;
    }

    public static class SampleAnnotationIndexQuery {
        private final byte[] annotationIndexMask; // byte[] = {mask , index}
        private final short consequenceTypeMask;
        private final byte biotypeMask;
        private final List<PopulationFrequencyQuery> populationFrequencyQueries;
        private final QueryOperation populationFrequencyQueryOperator;
        private final boolean populationFrequencyQueryPartial;

        public static class PopulationFrequencyQuery {
            private final int position;
            private final String study;
            private final String population;

            private final double minFreqInclusive;
            private final double maxFreqExclusive;
            private final byte minCodeInclusive;
            private final byte maxCodeExclusive;

            public PopulationFrequencyQuery(int position, String study, String population,
                                            double minFreqInclusive, double maxFreqExclusive,
                                            byte minCodeInclusive, byte maxCodeExclusive) {
                this.position = position;
                this.study = study;
                this.population = population;
                this.minFreqInclusive = minFreqInclusive;
                this.maxFreqExclusive = maxFreqExclusive;
                this.minCodeInclusive = minCodeInclusive;
                this.maxCodeExclusive = maxCodeExclusive;
            }

            public int getPosition() {
                return position;
            }

            public String getStudy() {
                return study;
            }

            public String getPopulation() {
                return population;
            }

            public double getMinFreqInclusive() {
                return minFreqInclusive;
            }

            public double getMaxFreqExclusive() {
                return maxFreqExclusive;
            }

            public byte getMinCodeInclusive() {
                return minCodeInclusive;
            }

            public byte getMaxCodeExclusive() {
                return maxCodeExclusive;
            }

            @Override
            public String toString() {
                return "PopulationFrequencyQuery{"
                        + "[" + position + "] population='" + study + ':' + population + '\''
                        + ", query [" + minFreqInclusive + ", " + maxFreqExclusive + ")"
                        + ", code [" + minCodeInclusive + ", " + maxCodeExclusive + ")"
                        + '}';
            }
        }


        public SampleAnnotationIndexQuery() {
            this.annotationIndexMask = new byte[]{0, 0};
            this.consequenceTypeMask = 0;
            this.biotypeMask = 0;
            this.populationFrequencyQueries = Collections.emptyList();
            this.populationFrequencyQueryOperator = QueryOperation.AND;
            this.populationFrequencyQueryPartial = true;
        }

        public SampleAnnotationIndexQuery(byte[] annotationIndexMask, short consequenceTypeMask, byte biotypeMask,
                                          QueryOperation populationFrequencyQueryOperator,
                                          List<PopulationFrequencyQuery> populationFrequencyQueries,
                                          boolean populationFrequencyQueryPartial) {
            this.annotationIndexMask = annotationIndexMask;
            this.consequenceTypeMask = consequenceTypeMask;
            this.biotypeMask = biotypeMask;
            this.populationFrequencyQueries = Collections.unmodifiableList(populationFrequencyQueries);
            this.populationFrequencyQueryOperator = populationFrequencyQueryOperator;
            this.populationFrequencyQueryPartial = populationFrequencyQueryPartial;
        }

        public byte getAnnotationIndexMask() {
            return annotationIndexMask[0];
        }

        public byte getAnnotationIndex() {
            return annotationIndexMask[1];
        }

        public short getConsequenceTypeMask() {
            return consequenceTypeMask;
        }

        public byte getBiotypeMask() {
            return biotypeMask;
        }

        public List<PopulationFrequencyQuery> getPopulationFrequencyQueries() {
            return populationFrequencyQueries;
        }

        public QueryOperation getPopulationFrequencyQueryOperator() {
            return populationFrequencyQueryOperator;
        }

        public boolean isPopulationFrequencyQueryPartial() {
            return populationFrequencyQueryPartial;
        }
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
        return annotationIndexQuery.getAnnotationIndexMask();
    }

    public byte getAnnotationIndex() {
        return annotationIndexQuery.getAnnotationIndex();
    }

    public boolean emptyAnnotationIndex() {
        return getAnnotationIndexMask() == EMPTY_MASK;
    }

    public SampleAnnotationIndexQuery getAnnotationIndexQuery() {
        return annotationIndexQuery;
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
                    query.annotationIndexQuery,
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
