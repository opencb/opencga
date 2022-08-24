package org.opencb.opencga.storage.core.variant.query.filters;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.List;
import java.util.function.Predicate;

public class PopulationFrequencyVariantFilter implements Predicate<Variant> {

    protected abstract static class PopulationFreqFilter {
        private final String study;
        private final String population;

        public PopulationFreqFilter(String study, String population) {
            this.study = study;
            this.population = population;
        }

        public final Boolean test(PopulationFrequency populationFrequency) {
            if (populationFrequency.getStudy().equals(study) && populationFrequency.getPopulation().equals(population)) {
                return testFreq(populationFrequency);
            } else {
                return null;
            }
        }

        abstract boolean onMissing();

        abstract boolean testFreq(PopulationFrequency populationFrequency);
    }

    protected static class AltFreqFilter extends PopulationFreqFilter {

        private final Predicate<Float> predicate;
        private final boolean onMissing;

        public AltFreqFilter(String study, String population, String op, float value) {
            super(study, population);
            predicate = NumericFilter.parse(op, value);
            onMissing = predicate.test(0f); // by default, alt freq is 0
        }

        @Override
        boolean onMissing() {
            return onMissing;
        }

        @Override
        boolean testFreq(PopulationFrequency populationFrequency) {
            return predicate.test(populationFrequency.getAltAlleleFreq());
        }
    }

    protected static class RefFreqFilter extends PopulationFreqFilter {

        private final Predicate<Float> predicate;
        private final boolean onMissing;

        public RefFreqFilter(String study, String population, String op, float value) {
            super(study, population);
            predicate = NumericFilter.parse(op, value);
            onMissing = predicate.test(1f); // by default, ref freq is 0
        }

        @Override
        boolean onMissing() {
            return onMissing;
        }

        @Override
        boolean testFreq(PopulationFrequency populationFrequency) {
            return predicate.test(populationFrequency.getRefAlleleFreq());
        }
    }

    protected static class MafFreqFilter extends PopulationFreqFilter {

        private final Predicate<Float> predicate;
        private final boolean onMissing;

        public MafFreqFilter(String study, String population, String op, float value) {
            super(study, population);
            predicate = NumericFilter.parse(op, value);
            onMissing = predicate.test(0f); // by default, MAF is 0
        }

        @Override
        boolean onMissing() {
            return onMissing;
        }

        @Override
        boolean testFreq(PopulationFrequency populationFrequency) {
            return predicate.test(Math.min(populationFrequency.getRefAlleleFreq(), populationFrequency.getAltAlleleFreq()));
        }
    }



    private final VariantQueryUtils.QueryOperation op;
    private final List<? extends PopulationFreqFilter> filters;
    private final boolean[] results;

    public PopulationFrequencyVariantFilter(VariantQueryUtils.QueryOperation operation, List<? extends PopulationFreqFilter> filters) {
        op = operation == null ? VariantQueryUtils.QueryOperation.OR : operation;
        this.filters = filters;
        results = new boolean[filters.size()];
    }

    @Override
    public boolean test(Variant variant) {
        // reset
        int i = 0;
        for (PopulationFreqFilter filter : filters) {
            results[i] = filter.onMissing();
            i++;
        }

        VariantAnnotation annotation = variant.getAnnotation();
        if (annotation != null) {
            List<PopulationFrequency> frequencies = annotation.getPopulationFrequencies();
            if (frequencies != null) {
                for (PopulationFrequency pf : frequencies) {
                    i = 0;
                    for (PopulationFreqFilter filter : filters) {
                        Boolean test = filter.test(pf);
                        if (test != null) {
                            results[i] = test;
                            break;
                        }
                        i++;
                    }
                }
            }
        }
        if (op == VariantQueryUtils.QueryOperation.OR) {
            for (boolean result : results) {
                if (result) {
                    // true on any match
                    return true;
                }
            }
            // no matches
            return false;
        } else {
            for (boolean result : results) {
                if (!result) {
                    // false on any miss
                    return false;
                }
            }
            // no misses
            return true;
        }
    }
}
