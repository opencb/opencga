package org.opencb.opencga.storage.hadoop.variant.index.query;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

import java.util.Collections;
import java.util.List;

public class SampleAnnotationIndexQuery {
    private final byte[] annotationIndexMask; // byte[] = {mask , index}
    private final short consequenceTypeMask;
    private final byte biotypeMask;
    private final byte clinicalMask;
    private final List<PopulationFrequencyQuery> populationFrequencyQueries;
    private final VariantQueryUtils.QueryOperation populationFrequencyQueryOperator;
    private final boolean populationFrequencyQueryPartial;

    public static class PopulationFrequencyQuery extends RangeQuery {
        private final int position;
        private final String study;
        private final String population;

        public PopulationFrequencyQuery(RangeQuery rangeQuery, int position, String study, String population) {
            super(rangeQuery.minValueInclusive, rangeQuery.maxValueExclusive,
                    rangeQuery.minCodeInclusive, rangeQuery.maxCodeExclusive,
                    rangeQuery.exactQuery);
            this.position = position;
            this.study = study;
            this.population = population;
        }

        public PopulationFrequencyQuery(int position, String study, String population,
                                        double minFreqInclusive, double maxFreqExclusive,
                                        byte minCodeInclusive, byte maxCodeExclusive) {
            super(minFreqInclusive, maxFreqExclusive, minCodeInclusive, maxCodeExclusive);
            this.position = position;
            this.study = study;
            this.population = population;
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

        public String getStudyPopulation() {
            return study + ":" + population;
        }

        @Override
        public String toString() {
            return "PopulationFrequencyQuery{"
                    + "[" + position + "] population='" + study + ':' + population + '\''
                    + ", query [" + minValueInclusive + ", " + maxValueExclusive + ")"
                    + ", code [" + minCodeInclusive + ", " + maxCodeExclusive + ")"
                    + ", exact: " + exactQuery
                    + '}';
        }
    }


    public SampleAnnotationIndexQuery() {
        this.annotationIndexMask = new byte[]{0, 0};
        this.consequenceTypeMask = 0;
        this.biotypeMask = 0;
        this.clinicalMask = 0;
        this.populationFrequencyQueries = Collections.emptyList();
        this.populationFrequencyQueryOperator = VariantQueryUtils.QueryOperation.AND;
        this.populationFrequencyQueryPartial = true;
    }

    public SampleAnnotationIndexQuery(byte[] annotationIndexMask, short consequenceTypeMask, byte biotypeMask,
                                      byte clinicalMask, VariantQueryUtils.QueryOperation populationFrequencyQueryOperator,
                                      List<PopulationFrequencyQuery> populationFrequencyQueries,
                                      boolean populationFrequencyQueryPartial) {
        this.annotationIndexMask = annotationIndexMask;
        this.consequenceTypeMask = consequenceTypeMask;
        this.biotypeMask = biotypeMask;
        this.clinicalMask = clinicalMask;
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

    public VariantQueryUtils.QueryOperation getPopulationFrequencyQueryOperator() {
        return populationFrequencyQueryOperator;
    }

    public boolean isPopulationFrequencyQueryPartial() {
        return populationFrequencyQueryPartial;
    }

    public byte getClinicalMask() {
        return clinicalMask;
    }

    public boolean isEmpty() {
        return getAnnotationIndexMask() == IndexUtils.EMPTY_MASK
                && biotypeMask == IndexUtils.EMPTY_MASK
                && consequenceTypeMask == IndexUtils.EMPTY_MASK
                && CollectionUtils.isEmpty(populationFrequencyQueries);
    }
}
