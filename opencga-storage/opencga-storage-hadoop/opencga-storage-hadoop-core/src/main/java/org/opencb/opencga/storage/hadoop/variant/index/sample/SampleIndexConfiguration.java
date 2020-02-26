package org.opencb.opencga.storage.hadoop.variant.index.sample;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SampleIndexConfiguration {

    public static final double[] QUAL_THRESHOLDS = new double[]{10, 20, 30};
    public static final double[] DP_THRESHOLDS = new double[]{5, 15, 30};

    private List<PopulationFrequencyRange> populationRanges;

    public SampleIndexConfiguration() {
        populationRanges = new ArrayList<>();
    }

    public static SampleIndexConfiguration defaultConfiguration() {
        return new SampleIndexConfiguration()
                .addPopulationRange(new PopulationFrequencyRange("1kG_phase3", "ALL"))
                .addPopulationRange(new PopulationFrequencyRange("GNOMAD_GENOMES", "ALL"));
    }

    public List<PopulationFrequencyRange> getPopulationRanges() {
        return populationRanges;
    }

    public SampleIndexConfiguration setPopulationRanges(List<PopulationFrequencyRange> populationRanges) {
        this.populationRanges = populationRanges;
        return this;
    }

    public SampleIndexConfiguration addPopulationRange(PopulationFrequencyRange populationRange) {
        if (populationRanges == null) {
            populationRanges = new ArrayList<>();
        }
        if (populationRanges.contains(populationRange)) {
            throw new IllegalArgumentException("Duplicated population '"
                    + populationRange.getStudyAndPopulation() + "' in SampleIndexConfiguration");
        }
        this.populationRanges.add(populationRange);
        return this;
    }

    public static class PopulationFrequencyRange {
        public static final double[] DEFAULT_THRESHOLDS = new double[]{0.001, 0.005, 0.01};
        private String study;
        private String population;
//        private float[] thresholds; // TODO: Make this configurable

        public PopulationFrequencyRange(String studyPopulation) {
            this.study = studyPopulation.split(":")[0];
            this.population = studyPopulation.split(":")[1];
        }

        public PopulationFrequencyRange(String study, String population) {
            this.study = study;
            this.population = population;
        }

        public String getStudy() {
            return study;
        }

        public String getStudyAndPopulation() {
            return study + ":" + population;
        }

        public PopulationFrequencyRange setStudy(String study) {
            this.study = study;
            return this;
        }

        public String getPopulation() {
            return population;
        }

        public PopulationFrequencyRange setPopulation(String population) {
            this.population = population;
            return this;
        }

        public double[] getThresholds() {
            return DEFAULT_THRESHOLDS;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PopulationFrequencyRange that = (PopulationFrequencyRange) o;
            return Objects.equals(study, that.study) && Objects.equals(population, that.population);
        }

        @Override
        public int hashCode() {
            return Objects.hash(study, population);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleIndexConfiguration that = (SampleIndexConfiguration) o;
        return Objects.equals(populationRanges, that.populationRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(populationRanges);
    }
}
