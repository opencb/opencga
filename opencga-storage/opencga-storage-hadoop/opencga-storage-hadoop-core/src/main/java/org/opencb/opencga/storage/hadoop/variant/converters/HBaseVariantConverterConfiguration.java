package org.opencb.opencga.storage.hadoop.variant.converters;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;

import java.util.List;

import static org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass.UNKNOWN_GENOTYPE;

public class HBaseVariantConverterConfiguration {

    public static final String MUTABLE_SAMPLES_POSITION = "mutableSamplesPosition";
    public static final String STUDY_NAME_AS_STUDY_ID = "studyNameAsStudyId";
    public static final String SIMPLE_GENOTYPES = "simpleGenotypes";

    private final VariantQueryProjection projection;

    private final boolean failOnWrongVariants;
    private final boolean failOnEmptyVariants;
    private final boolean studyNameAsStudyId;
    private final boolean simpleGenotypes;
    private final String unknownGenotype;
    private final boolean mutableSamplesPosition;
    private final List<String> format;
    private final boolean includeSampleId;
    private final boolean includeIndexStatus;

    public HBaseVariantConverterConfiguration(VariantQueryProjection projection, boolean failOnWrongVariants, boolean failOnEmptyVariants,
                                              boolean studyNameAsStudyId, boolean simpleGenotypes, String unknownGenotype,
                                              boolean mutableSamplesPosition, List<String> format, boolean includeSampleId,
                                              boolean includeIndexStatus) {
        this.projection = projection;
        this.failOnWrongVariants = failOnWrongVariants;
        this.failOnEmptyVariants = failOnEmptyVariants;
        this.studyNameAsStudyId = studyNameAsStudyId;
        this.simpleGenotypes = simpleGenotypes;
        this.unknownGenotype = unknownGenotype;
        this.mutableSamplesPosition = mutableSamplesPosition;
        this.format = format;
        this.includeSampleId = includeSampleId;
        this.includeIndexStatus = includeIndexStatus;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Configuration configuration) {
        Builder builder = new Builder();

        if (StringUtils.isNotEmpty(configuration.get(MUTABLE_SAMPLES_POSITION))) {
            builder.setMutableSamplesPosition(configuration.getBoolean(MUTABLE_SAMPLES_POSITION, true));
        }
        if (StringUtils.isNotEmpty(configuration.get(STUDY_NAME_AS_STUDY_ID))) {
            builder.setStudyNameAsStudyId(configuration.getBoolean(STUDY_NAME_AS_STUDY_ID, false));
        }
        if (StringUtils.isNotEmpty(configuration.get(SIMPLE_GENOTYPES))) {
            builder.setSimpleGenotypes(configuration.getBoolean(SIMPLE_GENOTYPES, false));
        }
        builder.setUnknownGenotype(configuration.get(VariantQueryParam.UNKNOWN_GENOTYPE.key()));
        return builder;
    }

    public static Builder builder(QueryOptions options) {
        Builder builder = new Builder();
        if (options == null) {
            return builder;
        }

        if (StringUtils.isNotEmpty(options.getString(MUTABLE_SAMPLES_POSITION))) {
            builder.setMutableSamplesPosition(options.getBoolean(MUTABLE_SAMPLES_POSITION, true));
        }
        if (StringUtils.isNotEmpty(options.getString(STUDY_NAME_AS_STUDY_ID))) {
            builder.setStudyNameAsStudyId(options.getBoolean(STUDY_NAME_AS_STUDY_ID, false));
        }
        if (StringUtils.isNotEmpty(options.getString(SIMPLE_GENOTYPES))) {
            builder.setSimpleGenotypes(options.getBoolean(SIMPLE_GENOTYPES, false));
        }
        builder.setUnknownGenotype(options.getString(VariantQueryParam.UNKNOWN_GENOTYPE.key()));
        return builder;
    }

    public VariantQueryProjection getProjection() {
        return projection;
    }

    public boolean getStudyNameAsStudyId() {
        return studyNameAsStudyId;
    }

    public boolean getSimpleGenotypes() {
        return simpleGenotypes;
    }

    public boolean getFailOnWrongVariants() {
        return failOnWrongVariants;
    }

    public boolean getFailOnEmptyVariants() {
        return failOnEmptyVariants;
    }

    public String getUnknownGenotype() {
        return unknownGenotype;
    }

    public boolean getMutableSamplesPosition() {
        return mutableSamplesPosition;
    }

    /**
     * Format of the converted variants. Discard other values.
     * @see org.opencb.opencga.storage.core.variant.query.VariantQueryUtils#getIncludeFormats(Query)
     * @return expected format
     */
    public List<String> getFormat() {
        return format;
    }

    public boolean getIncludeSampleId() {
        return includeSampleId;
    }

    public boolean getIncludeIndexStatus() {
        return includeIndexStatus;
    }

    public static class Builder {
        private VariantQueryProjection projection;
        private boolean studyNameAsStudyId = false;
        private boolean simpleGenotypes = false;
        private boolean failOnWrongVariants = HBaseToVariantConverter.isFailOnWrongVariants();
        private boolean failOnEmptyVariants = false;
        private String unknownGenotype = UNKNOWN_GENOTYPE;
        private boolean mutableSamplesPosition = true;
        private List<String> format;
        private boolean includeSampleId;
        private boolean includeIndexStatus;

        public Builder setProjection(VariantQueryProjection projection) {
            this.projection = projection;
            return this;
        }

        public Builder setStudyNameAsStudyId(boolean studyNameAsStudyId) {
            this.studyNameAsStudyId = studyNameAsStudyId;
            return this;
        }

        public Builder setSimpleGenotypes(boolean simpleGenotypes) {
            this.simpleGenotypes = simpleGenotypes;
            return this;
        }

        public Builder setFailOnWrongVariants(boolean failOnWrongVariants) {
            this.failOnWrongVariants = failOnWrongVariants;
            return this;
        }

        public Builder setFailOnEmptyVariants(boolean failOnEmptyVariants) {
            this.failOnEmptyVariants = failOnEmptyVariants;
            return this;
        }

        public Builder setUnknownGenotype(String unknownGenotype) {
            if (StringUtils.isEmpty(unknownGenotype)) {
                this.unknownGenotype = UNKNOWN_GENOTYPE;
            } else {
                this.unknownGenotype = unknownGenotype;
            }
            return this;
        }

        public Builder setMutableSamplesPosition(boolean mutableSamplesPosition) {
            this.mutableSamplesPosition = mutableSamplesPosition;
            return this;
        }

        public Builder setFormat(List<String> format) {
            this.format = format;
            return this;
        }

        public Builder setIncludeSampleId(boolean includeSampleId) {
            this.includeSampleId = includeSampleId;
            return this;
        }

        public Builder setIncludeIndexStatus(boolean includeIndexStatus) {
            this.includeIndexStatus = includeIndexStatus;
            return this;
        }

        public HBaseVariantConverterConfiguration build() {
            return new HBaseVariantConverterConfiguration(
                    projection, failOnWrongVariants,
                    failOnEmptyVariants,
                    studyNameAsStudyId,
                    simpleGenotypes,
                    unknownGenotype,
                    mutableSamplesPosition,
                    format,
                    includeSampleId,
                    includeIndexStatus);
        }
    }
}
