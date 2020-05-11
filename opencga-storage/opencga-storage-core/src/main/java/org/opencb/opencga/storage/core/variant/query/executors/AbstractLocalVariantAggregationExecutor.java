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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public abstract class AbstractLocalVariantAggregationExecutor extends VariantAggregationExecutor {

    protected abstract static class FieldVariantAccumulator<T> {
        private FieldVariantAccumulator<T> nestedFieldAccumulator;

        protected FieldVariantAccumulator(FieldVariantAccumulator<T> nestedFieldAccumulator) {
            this.nestedFieldAccumulator = nestedFieldAccumulator;
        }

        public FieldVariantAccumulator<T> setNestedFieldAccumulator(FieldVariantAccumulator<T> nestedFieldAccumulator) {
            this.nestedFieldAccumulator = nestedFieldAccumulator;
            return this;
        }

        /**
         * Get field name.
         * @return Field name
         */
        public abstract String getName();

        /**
         * Prepare (if required) the list of buckets for this field.
         * @return predefined list of buckets.
         */
        public FacetField createField() {
            return new FacetField(getName(), 0, prepareBuckets());
        }

        /**
         * Prepare (if required) the list of buckets for this field.
         * @return predefined list of buckets.
         */
        public final List<FacetField.Bucket> prepareBuckets() {
            List<FacetField.Bucket> valueBuckets = prepareBuckets1();
            for (FacetField.Bucket bucket : valueBuckets) {
                if (nestedFieldAccumulator != null) {
                    bucket.setFacetFields(Collections.singletonList(nestedFieldAccumulator.createField()));
                }
            }
            return valueBuckets;
        }

        protected final FacetField.Bucket addBucket(FacetField field, String value) {
            FacetField.Bucket bucket;
            bucket = new FacetField.Bucket(value, 0, null);
            if (nestedFieldAccumulator != null) {
                bucket.setFacetFields(Collections.singletonList(nestedFieldAccumulator.createField()));
            }
            field.getBuckets().add(bucket);
            return bucket;
        }

        protected abstract List<FacetField.Bucket> prepareBuckets1();

        public void cleanEmptyBuckets(FacetField field) {
            field.getBuckets().removeIf(bucket -> bucket.getCount() == 0);
            if (nestedFieldAccumulator != null) {
                for (FacetField.Bucket bucket : field.getBuckets()) {
                    nestedFieldAccumulator.cleanEmptyBuckets(bucket.getFacetFields().get(0));
                }
            }
        }

        /**
         * Accumulate variant in the given field.
         * @param field   Field
         * @param variant Variant
         */
        public final void accumulate(FacetField field, T variant) {
            List<FacetField.Bucket> buckets = getBuckets(field, variant);
            if (buckets == null || buckets.isEmpty()) {
                return;
            }
            field.addCount(1);
            for (FacetField.Bucket bucket : buckets) {
                bucket.addCount(1);
                if (nestedFieldAccumulator != null) {
                    nestedFieldAccumulator.accumulate(bucket.getFacetFields().get(0), variant);
                }
            }
        }

        protected abstract List<FacetField.Bucket> getBuckets(FacetField field, T variant);
    }

    protected static class VariantChromDensityAccumulator<T> extends ChromDensityAccumulator<Variant> {

        protected VariantChromDensityAccumulator(VariantStorageMetadataManager metadataManager, Region region,
                                                 FieldVariantAccumulator<Variant> nestedFieldAccumulator, int step) {
            super(metadataManager, region, nestedFieldAccumulator, step, Variant::getStart);
        }
    }

    protected static class ChromDensityAccumulator<T> extends FieldVariantAccumulator<T> {
        private final VariantStorageMetadataManager metadataManager;
        private final Region region;
        private final int step;
        private final int numSteps;
        private final Function<T, Integer> getStart;

        public ChromDensityAccumulator(VariantStorageMetadataManager metadataManager, Region region,
                                        FieldVariantAccumulator<T> nestedFieldAccumulator, int step, Function<T, Integer> getStart) {
            super(nestedFieldAccumulator);
            this.metadataManager = metadataManager;
            this.region = region;
            this.step = step;
            this.getStart = getStart;

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
        public List<FacetField.Bucket> prepareBuckets1() {
            List<FacetField.Bucket> valueBuckets = new ArrayList<>(numSteps);
            for (int i = 0; i < numSteps; i++) {
                valueBuckets.add(new FacetField.Bucket(String.valueOf(i * step + region.getStart()), 0, null));
            }
            return valueBuckets;
        }

        @Override
        protected List<FacetField.Bucket> getBuckets(FacetField field, T variant) {
            int idx = (getStart(variant) - region.getStart()) / step;
            if (idx < numSteps) {
                return Collections.singletonList(field.getBuckets().get(idx));
            } else {
                return null;
            }
        }

        protected Integer getStart(T variant) {
            return getStart.apply(variant);
        }
    }

    protected static class VariantTypeAccumulator<T> extends FieldVariantAccumulator<T> {

        private final Function<T, VariantType> getType;

        public VariantTypeAccumulator(Function<T, VariantType> getType) {
            this(getType, null);
            // TODO: Accept subset of variant type
        }

        public VariantTypeAccumulator(Function<T, VariantType> getType, FieldVariantAccumulator<T> nestedFieldAccumulator) {
            super(nestedFieldAccumulator);
            this.getType = getType;
        }

        @Override
        public String getName() {
            return VariantField.TYPE.fieldName();
        }

        @Override
        public List<FacetField.Bucket> prepareBuckets1() {
            List<FacetField.Bucket> buckets = new ArrayList<>(VariantType.values().length);
            for (VariantType variantType : VariantType.values()) {
                buckets.add(new FacetField.Bucket(variantType.name(), 0, null));
            }
            return buckets;
        }

        @Override
        protected List<FacetField.Bucket> getBuckets(FacetField field, T variant) {
            return Collections.singletonList(field.getBuckets().get(getType(variant).ordinal()));
        }

        protected VariantType getType(T variant) {
            return getType.apply(variant);
        }
    }

    protected static class ChromosomeAccumulator extends CategoricalAccumulator<Variant> {

        public ChromosomeAccumulator() {
            this(null);
        }

        public ChromosomeAccumulator(FieldVariantAccumulator<Variant> nestedFieldAccumulator) {
            super(v -> Collections.singletonList(v.getChromosome()), VariantField.CHROMOSOME.fieldName(), nestedFieldAccumulator);
        }
    }

    protected static class CategoricalAccumulator<T> extends FieldVariantAccumulator<T> {

        private final Function<T, Collection<String>> getCategory;
        private final String name;

        public CategoricalAccumulator(Function<T, Collection<String>> getCategory, String name) {
            this(getCategory, name, null);
            // TODO: Accept subset of categories
        }

        public CategoricalAccumulator(Function<T, Collection<String>> getCategory, String name, FieldVariantAccumulator<T> nestedFieldAccumulator) {
            super(nestedFieldAccumulator);
            this.getCategory = getCategory;
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public List<FacetField.Bucket> prepareBuckets1() {
            List<FacetField.Bucket> buckets = new ArrayList<>();
            return buckets;
        }

        @Override
        protected List<FacetField.Bucket> getBuckets(FacetField field, T variant) {
            Collection<String> values = getCategory.apply(variant);
            List<FacetField.Bucket> buckets = new ArrayList<>();
            for (String value : values) {
                FacetField.Bucket bucket = null;
                for (FacetField.Bucket thisBucket : field.getBuckets()) {
                    if (thisBucket.getValue().equals(value)) {
                        buckets.add(thisBucket);
                        bucket = thisBucket;
                    }
                }
                if (bucket == null) {
                    buckets.add(addBucket(field, value));
                }
            }
            return buckets;
        }

    }

}
