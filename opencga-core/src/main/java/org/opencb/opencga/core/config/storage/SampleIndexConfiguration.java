package org.opencb.opencga.core.config.storage;

import htsjdk.variant.vcf.VCFConstants;
import org.opencb.biodata.models.variant.StudyEntry;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SampleIndexConfiguration {
    public static final int DEFAULT_FILE_POSITION_SIZE_BITS = 4;
    private static final double[] QUAL_THRESHOLDS = new double[]{10, 20, 30};
    private static final double[] DP_THRESHOLDS = new double[]{5, 10, 15, 20, 30, 40, 50};

    private final List<PopulationFrequencyRange> populationRanges = new ArrayList<>();
    private final FileIndexConfiguration fileIndexConfiguration = new FileIndexConfiguration();

    public static SampleIndexConfiguration defaultConfiguration() {
        SampleIndexConfiguration sampleIndexConfiguration = new SampleIndexConfiguration()
                .addPopulationRange(new PopulationFrequencyRange("1kG_phase3", "ALL"))
                .addPopulationRange(new PopulationFrequencyRange("GNOMAD_GENOMES", "ALL"))
                .addFileIndexField(new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.FILE,
                        StudyEntry.FILTER,
                        IndexFieldConfiguration.Type.CATEGORICAL,
                        VCFConstants.PASSES_FILTERS_v4))
                .addFileIndexField(new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.FILE, StudyEntry.QUAL, QUAL_THRESHOLDS).setNullable(false))
                .addFileIndexField(new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.SAMPLE, VCFConstants.DEPTH_KEY, DP_THRESHOLDS).setNullable(false));

        sampleIndexConfiguration.getFileIndexConfiguration()
                .setFilePositionBits(DEFAULT_FILE_POSITION_SIZE_BITS);

        // Ensure backward compatibility with these two params:
        sampleIndexConfiguration.addFileIndexField(new IndexFieldConfiguration(
                IndexFieldConfiguration.Source.SAMPLE, "padding", IndexFieldConfiguration.Type.CATEGORICAL,
                "add_two_extra_bits", "to_allow_backward", "compatibility"));
        sampleIndexConfiguration.getFileIndexConfiguration().setFixedFieldsFirst(false);
        return sampleIndexConfiguration;
    }

    public void validate() {
        for (IndexFieldConfiguration customField : fileIndexConfiguration.getCustomFields()) {
            customField.validate();
        }
        for (PopulationFrequencyRange populationRange : populationRanges) {
            populationRange.validate();
        }
    }

    public static class FileIndexConfiguration {

        private final List<IndexFieldConfiguration> customFields = new ArrayList<>();
        private int filePositionBits = DEFAULT_FILE_POSITION_SIZE_BITS;
        private boolean fixedFieldsFirst = true;

        public FileIndexConfiguration() {
        }

        public FileIndexConfiguration(int filePositionBits, boolean fixedFieldsFirst) {
            this.filePositionBits = filePositionBits;
            this.fixedFieldsFirst = fixedFieldsFirst;
        }

        public List<IndexFieldConfiguration> getCustomFields() {
            return customFields;
        }

        public int getFilePositionBits() {
            return filePositionBits;
        }

        public FileIndexConfiguration setFilePositionBits(int filePositionBits) {
            this.filePositionBits = filePositionBits;
            return this;
        }

        public boolean isFixedFieldsFirst() {
            return fixedFieldsFirst;
        }

        public FileIndexConfiguration setFixedFieldsFirst(boolean fixedFieldsFirst) {
            this.fixedFieldsFirst = fixedFieldsFirst;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileIndexConfiguration that = (FileIndexConfiguration) o;
            return filePositionBits == that.filePositionBits
                    && fixedFieldsFirst == that.fixedFieldsFirst
                    && Objects.equals(customFields, that.customFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(customFields, filePositionBits, fixedFieldsFirst);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("FileIndexConfiguration{");
            sb.append("customFields=").append(customFields);
            sb.append(", filePositionBits=").append(filePositionBits);
            sb.append(", fixedFieldsFirst=").append(fixedFieldsFirst);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class PopulationFrequencyRange extends IndexFieldConfiguration {
        @Deprecated
        //TODO: This field should be private
        public static final double[] DEFAULT_THRESHOLDS = new double[]{0.001, 0.005, 0.01};
        private String study;
        private String population;

        public PopulationFrequencyRange(String studyPopulation) {
            super(Source.ANNOTATION, studyPopulation, DEFAULT_THRESHOLDS);
            this.study = studyPopulation.split(":")[0];
            this.population = studyPopulation.split(":")[1];
            setNullable(false);
        }

        public PopulationFrequencyRange(String study, String population) {
            super(Source.ANNOTATION, study + ":" + population, DEFAULT_THRESHOLDS);
            this.study = study;
            this.population = population;
            setNullable(false);
        }

        @ConstructorProperties({"source", "key", "type"})
        protected PopulationFrequencyRange(Source source, String key, Type type) {
            super(source, key, type);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PopulationFrequencyRange that = (PopulationFrequencyRange) o;
            return super.equals(that) && Objects.equals(study, that.study) && Objects.equals(population, that.population);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), study, population);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("PopulationFrequencyRange{");
            sb.append("study='").append(study).append('\'');
            sb.append(", population='").append(population).append('\'');
            sb.append(", source=").append(source);
            sb.append(", key='").append(key).append('\'');
            sb.append(", type=").append(type);
            sb.append(", thresholds=").append(Arrays.toString(thresholds));
            sb.append(", values=").append(Arrays.toString(values));
            sb.append('}');
            return sb.toString();
        }
    }


    public List<SampleIndexConfiguration.PopulationFrequencyRange> getPopulationRanges() {
        return populationRanges;
    }

    public SampleIndexConfiguration addPopulationRange(SampleIndexConfiguration.PopulationFrequencyRange populationRange) {
        if (populationRanges.contains(populationRange)) {
            throw new IllegalArgumentException("Duplicated population '"
                    + populationRange.getStudyAndPopulation() + "' in SampleIndexConfiguration");
        }
        this.populationRanges.add(populationRange);
        return this;
    }

    public List<IndexFieldConfiguration> getFileIndexFieldsConfiguration() {
        return fileIndexConfiguration.getCustomFields();
    }

    public FileIndexConfiguration getFileIndexConfiguration() {
        return fileIndexConfiguration;
    }


    public SampleIndexConfiguration addFileIndexField(IndexFieldConfiguration fileIndex) {
        if (fileIndexConfiguration.getCustomFields().contains(fileIndex)) {
            throw new IllegalArgumentException("Duplicated file index '"
                    + fileIndex.getKey() + "' in SampleIndexConfiguration");
        }
        this.fileIndexConfiguration.getCustomFields().add(fileIndex);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SampleIndexConfiguration that = (SampleIndexConfiguration) o;
        return Objects.equals(populationRanges, that.populationRanges)
                && Objects.equals(fileIndexConfiguration, that.fileIndexConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(populationRanges, fileIndexConfiguration);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleIndexConfiguration{");
        sb.append("populationRanges=").append(populationRanges);
        sb.append(", fileIndexConfiguration=").append(fileIndexConfiguration);
        sb.append('}');
        return sb.toString();
    }
}
