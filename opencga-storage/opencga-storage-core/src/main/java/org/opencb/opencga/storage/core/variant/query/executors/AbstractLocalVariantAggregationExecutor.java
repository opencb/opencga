package org.opencb.opencga.storage.core.variant.query.executors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractLocalVariantAggregationExecutor extends VariantAggregationExecutor {

    protected interface FieldVariantAccumulator<T> {
        /**
         * Get field name.
         * @return Field name
         */
        String getName();

        /**
         * Prepare (if required) the list of buckets for this field.
         * @return predefined list of buckets.
         */
        default FacetField createField() {
            return new FacetField(getName(), 0, prepareBuckets());
        }

        /**
         * Prepare (if required) the list of buckets for this field.
         * @return predefined list of buckets.
         */
        List<FacetField.Bucket> prepareBuckets();

        default void cleanEmptyBuckets(FacetField field) {
            field.getBuckets().removeIf(bucket -> bucket.getCount() == 0);
        }

        /**
         * Accumulate variant in the given field.
         * @param field   Field
         * @param variant Variant
         */
        void accumulate(FacetField field, T variant);
    }

    protected static class ChromDensityAccumulator<T> extends AbstractChromDensityAccumulator<Variant> {

        protected ChromDensityAccumulator(VariantStorageMetadataManager metadataManager, Region region,
                                        FieldVariantAccumulator<Variant> nestedFieldAccumulator, int step) {
            super(metadataManager, region, nestedFieldAccumulator, step);
        }

        @Override
        protected Integer getStart(Variant variant) {
            return variant.getStart();
        }
    }

    protected abstract static class AbstractChromDensityAccumulator<T> implements FieldVariantAccumulator<T> {
        private final VariantStorageMetadataManager metadataManager;
        private final Region region;
        private final FieldVariantAccumulator<T> nestedFieldAccumulator;
        private final int step;
        private final int numSteps;

        private AbstractChromDensityAccumulator(VariantStorageMetadataManager metadataManager, Region region,
                                                FieldVariantAccumulator<T> nestedFieldAccumulator, int step) {
            this.metadataManager = metadataManager;
            this.region = region;
            this.nestedFieldAccumulator = nestedFieldAccumulator;
            this.step = step;

            if (region.getEnd() == Integer.MAX_VALUE) {
                for (Integer studyId : this.metadataManager.getStudyIds()) {
                    StudyMetadata studyMetadata = this.metadataManager.getStudyMetadata(studyId);
                    VariantFileHeaderComplexLine contig = studyMetadata.getVariantHeaderLine("contig", region.getChromosome());
                    if (contig == null) {
                        contig = studyMetadata.getVariantHeaderLine("contig", "chr" + region.getChromosome());
                    }
                    if (contig != null) {
                        String length = contig.getGenericFields().get("length");
                        if (StringUtils.isNotEmpty(length) && StringUtils.isNumeric(length)) {
                            region.setEnd(Integer.parseInt(length));
                            break;
                        }
                    }
                }
            }
            if (region.getStart() == 0) {
                region.setStart(1);
            }

            int regionLength = region.getEnd() - region.getStart();
            if (regionLength != Integer.MAX_VALUE) {
                regionLength++;
            }
            numSteps = regionLength / step + 1;
        }

        @Override
        public String getName() {
            return VariantField.START.fieldName();
        }

        @Override
        public FacetField createField() {
            return new FacetField(VariantField.START.fieldName(), 0,
                    prepareBuckets())
                    .setStart(region.getStart())
                    .setEnd(region.getEnd())
                    .setStep(step);
        }

        @Override
        public List<FacetField.Bucket> prepareBuckets() {
            List<FacetField.Bucket> valueBuckets = new ArrayList<>(numSteps);
            for (int i = 0; i < numSteps; i++) {
                FacetField.Bucket bucket = new FacetField.Bucket(String.valueOf(i * step + region.getStart()), 0, null);
                if (nestedFieldAccumulator != null) {
                    bucket.setFacetFields(Collections.singletonList(nestedFieldAccumulator.createField()));
                }
                valueBuckets.add(bucket);
            }
            return valueBuckets;
        }

        @Override
        public void accumulate(FacetField field, T variant) {
            int idx = (getStart(variant) - region.getStart()) / step;
            if (idx < numSteps) {
                field.addCount(1);
                FacetField.Bucket bucket = field.getBuckets().get(idx);
                bucket.addCount(1);
                if (nestedFieldAccumulator != null) {
                    nestedFieldAccumulator.accumulate(bucket.getFacetFields().get(0), variant);
                }
            }
        }

        protected abstract Integer getStart(T variant);

        @Override
        public void cleanEmptyBuckets(FacetField field) {
            field.getBuckets().removeIf(bucket -> bucket.getCount() == 0);
            if (nestedFieldAccumulator != null) {
                for (FacetField.Bucket bucket : field.getBuckets()) {
                    nestedFieldAccumulator.cleanEmptyBuckets(bucket.getFacetFields().get(0));
                }
            }
        }
    }

    protected static class VariantTypeAccumulator extends AbstractVariantTypeAccumulator<Variant> {
        @Override
        protected VariantType getType(Variant variant) {
            return variant.getType();
        }
    }

    protected abstract static class AbstractVariantTypeAccumulator<T> implements FieldVariantAccumulator<T> {

        private AbstractVariantTypeAccumulator() {
            // TODO: Accept subset of variant type
        }

        @Override
        public String getName() {
            return VariantField.TYPE.fieldName();
        }

        @Override
        public List<FacetField.Bucket> prepareBuckets() {
            List<FacetField.Bucket> buckets = new ArrayList<>(VariantType.values().length);
            for (VariantType variantType : VariantType.values()) {
                buckets.add(new FacetField.Bucket(variantType.name(), 0, null));
            }
            return buckets;
        }

        @Override
        public void accumulate(FacetField field, T variant) {
            field.addCount(1);
            field.getBuckets().get(getType(variant).ordinal()).addCount(1);
        }

        protected abstract VariantType getType(T variant);
    }


}
