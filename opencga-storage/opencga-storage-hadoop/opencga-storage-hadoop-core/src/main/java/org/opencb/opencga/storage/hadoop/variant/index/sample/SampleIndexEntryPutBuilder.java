package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.io.bit.BitOutputStream;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.toRowKey;

public class SampleIndexEntryPutBuilder {

    private final int sampleId;
    private final String chromosome;
    private final int position;

    private final Map<String, SampleIndexGtEntryBuilder> gts;
    private final SampleIndexVariantBiConverter variantConverter;
    private static final byte[] COLUMN_FAMILY = GenomeHelper.COLUMN_FAMILY_BYTES;
    private final SampleIndexSchema schema;
    private final FileIndexSchema fileIndex;
    private final SampleVariantIndexEntry.SampleVariantIndexEntryComparator comparator;
    private final boolean orderedInput;
    private final boolean multiFileSample;

    public SampleIndexEntryPutBuilder(int sampleId, Variant variant, SampleIndexSchema schema,
                                      boolean orderedInput, boolean multiFileSample) {
        this(sampleId, variant.getChromosome(), SampleIndexSchema.getChunkStart(variant.getStart()), schema,
                orderedInput, multiFileSample);
    }

    /**
     * Create a SampleIndexEntryPutBuilder for one specific Sample and chunk region (chr+pos).
     *
     * @param sampleId          Sample ID
     * @param chromosome        Chunk region Chromosome
     * @param position          Chunk region position (Aligned to chunk start)
     * @param schema            SampleIndex schema
     * @param orderedInput      This builder will be fed with ordered variants
     *                          as {@link SampleIndexSchema#INTRA_CHROMOSOME_VARIANT_COMPARATOR}.
     * @param multiFileSample   Sample is configured as "multi-file" sample.
     */
    public SampleIndexEntryPutBuilder(int sampleId, String chromosome, int position, SampleIndexSchema schema,
                                      boolean orderedInput, boolean multiFileSample) {
        this.sampleId = sampleId;
        this.chromosome = chromosome;
        this.position = position;
        this.orderedInput = orderedInput;
        this.multiFileSample = multiFileSample;
        gts = new HashMap<>();
        variantConverter = new SampleIndexVariantBiConverter(schema);
        this.schema = schema;
        fileIndex = this.schema.getFileIndex();
        comparator = new SampleVariantIndexEntry.SampleVariantIndexEntryComparator(schema);
    }

    public SampleIndexEntryPutBuilder(int sampleId, String chromosome, int position, SampleIndexSchema schema,
                                      Map<String, TreeSet<SampleVariantIndexEntry>> map) {
        // As there is already present data, this won't be an ordered input.
        this(sampleId, chromosome, position, schema, false, true);
        for (Map.Entry<String, TreeSet<SampleVariantIndexEntry>> entry : map.entrySet()) {
            gts.put(entry.getKey(), new SampleIndexGtEntryBuilderTreeSet(entry.getKey(), entry.getValue()));
        }
    }

    public boolean add(String gt, SampleVariantIndexEntry variantIndexEntry) {
        return get(gt).add(variantIndexEntry);
    }

    private SampleIndexGtEntryBuilder get(String gt) {
        return gts.computeIfAbsent(gt, gt1 -> orderedInput
//                ? new SampleIndexGtEntryBuilderAssumeOrdered(gt1, comparator)
                ? new SampleIndexGtEntryBuilderWithPartialBuilds(gt1)
                : new SampleIndexGtEntryBuilderTreeSet(gt1)
        );
    }

    public boolean containsVariant(SampleVariantIndexEntry variantIndexEntry) {
        for (Map.Entry<String, SampleIndexGtEntryBuilder> entry : gts.entrySet()) {

            if (entry.getValue().containsVariant(variantIndexEntry)) {
                return true;
            }
        }
        return false;
    }

    public Set<String> getGtSet() {
        return gts.keySet();
    }

    public boolean isEmpty() {
        return gts.isEmpty();
    }

    public Put build() {
        byte[] rk = toRowKey(sampleId, chromosome, position);
        Put put = new Put(rk);
        if (gts.isEmpty()) {
            return put;
        }

        for (SampleIndexGtEntryBuilder gtBuilder : gts.values()) {
            gtBuilder.build(put);
        }
        int discrepancies = 0;

        if (multiFileSample) {
            // Check for discrepancies
            Iterator<SampleIndexGtEntryBuilder> iterator = gts.values().iterator();
            while (iterator.hasNext()) {
                SampleIndexGtEntryBuilder gt = iterator.next();
                iterator.remove();
                for (SampleIndexGtEntryBuilder otherGt : gts.values()) {
                    discrepancies += otherGt.containsVariants(gt);
                }
            }
        }
        put.addColumn(COLUMN_FAMILY, SampleIndexSchema.toGenotypeDiscrepanciesCountColumn(), Bytes.toBytes(discrepancies));

        return put;
    }

    private abstract class SampleIndexGtEntryBuilder {
        protected final String gt;

        SampleIndexGtEntryBuilder(String gt) {
            this.gt = gt;
        }

        public String getGt() {
            return gt;
        }

        public abstract Collection<SampleVariantIndexEntry> getEntries();

        public abstract boolean add(SampleVariantIndexEntry variantIndexEntry);

        public abstract boolean containsVariant(SampleVariantIndexEntry variantIndexEntry);

        public abstract int containsVariants(SampleIndexGtEntryBuilder entries);

        public void build(Put put) {
            Collection<SampleVariantIndexEntry> gtEntries = getEntries();

            BitBuffer fileIndexBuffer = new BitBuffer(fileIndex.getBitsLength() * gtEntries.size());
            int offset = 0;

            SampleVariantIndexEntry prev = null;
            List<Variant> variants = new ArrayList<>(gtEntries.size());
            for (SampleVariantIndexEntry gtEntry : gtEntries) {
                Variant variant = gtEntry.getVariant();
                if (prev == null || !prev.getVariant().sameGenomicVariant(variant)) {
                    variants.add(variant);
                } else {
                    // Mark previous variant as MultiFile
                    fileIndex.setMultiFile(fileIndexBuffer, offset - fileIndex.getBitsLength());
                }
                fileIndexBuffer.setBitBuffer(gtEntry.getFileIndex(), offset);
                offset += fileIndex.getBitsLength();
                prev = gtEntry;
            }

            byte[] variantsBytes = variantConverter.toBytes(variants);

            put.addColumn(COLUMN_FAMILY, SampleIndexSchema.toGenotypeColumn(gt), variantsBytes);
            put.addColumn(COLUMN_FAMILY, SampleIndexSchema.toGenotypeCountColumn(gt), Bytes.toBytes(variants.size()));
            put.addColumn(COLUMN_FAMILY, SampleIndexSchema.toFileIndexColumn(gt), fileIndexBuffer.getBuffer());
        }
    }

    private class SampleIndexGtEntryBuilderTreeSet extends SampleIndexGtEntryBuilder {
        private final TreeSet<SampleVariantIndexEntry> entries;

        SampleIndexGtEntryBuilderTreeSet(String gt) {
            super(gt);
            entries = new TreeSet<>(comparator);
        }

        SampleIndexGtEntryBuilderTreeSet(String gt, TreeSet<SampleVariantIndexEntry> entries) {
            super(gt);
            this.entries = entries;
        }

        @Override
        public Collection<SampleVariantIndexEntry> getEntries() {
            return entries;
        }

        @Override
        public boolean add(SampleVariantIndexEntry variantIndexEntry) {
            return entries.add(variantIndexEntry);
        }

        @Override
        public boolean containsVariant(SampleVariantIndexEntry variantIndexEntry) {
            SampleVariantIndexEntry lower = entries.lower(variantIndexEntry);
            if (lower != null && lower.getVariant().sameGenomicVariant(variantIndexEntry.getVariant())) {
                return true;
            }
            SampleVariantIndexEntry ceiling = entries.ceiling(variantIndexEntry);
            if (ceiling != null && ceiling.getVariant().sameGenomicVariant(variantIndexEntry.getVariant())) {
                return true;
            }
            return false;
        }

        @Override
        public int containsVariants(SampleIndexGtEntryBuilder other) {
            int c = 0;
            for (SampleVariantIndexEntry entry : other.getEntries()) {
                if (containsVariant(entry)) {
                    c++;
                }
            }
            return c;
        }
    }

    private class SampleIndexGtEntryBuilderAssumeOrdered extends SampleIndexGtEntryBuilder {
        protected final List<SampleVariantIndexEntry> entries;
        protected SampleVariantIndexEntry lastEntry;

        SampleIndexGtEntryBuilderAssumeOrdered(String gt) {
            super(gt);
            entries = new ArrayList<>(1000);
        }

        @Override
        public Collection<SampleVariantIndexEntry> getEntries() {
            return entries;
        }

        @Override
        public boolean add(SampleVariantIndexEntry variantIndexEntry) {
            if (lastEntry != null) {
                if (comparator.compare(lastEntry, variantIndexEntry) >= 0) {
                    throw new IllegalArgumentException("Using unordered input");
                }
            }
            lastEntry = variantIndexEntry;

            return entries.add(variantIndexEntry);
        }

        @Override
        public boolean containsVariant(SampleVariantIndexEntry variantIndexEntry) {
            for (SampleVariantIndexEntry entry : entries) {
                if (entry.getVariant().sameGenomicVariant(variantIndexEntry.getVariant())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int containsVariants(SampleIndexGtEntryBuilder other) {
            TreeSet<SampleVariantIndexEntry> tree = new TreeSet<>(comparator);
            tree.addAll(this.entries);
            return new SampleIndexGtEntryBuilderTreeSet(getGt(), tree).containsVariants(other);
        }
    }

    private class SampleIndexGtEntryBuilderWithPartialBuilds extends SampleIndexGtEntryBuilderAssumeOrdered {

        private final int threshold;
        private SampleVariantIndexEntry prev = null;
        // Variants is a shared object. No problem for the GC.
        private final ArrayList<Variant> variants = new ArrayList<>(0);
        // This is the real issue. This might produce the "too many objects" problem. Need to run "partial builds" from time to time.
        private final BitOutputStream fileIndexBuffer = new BitOutputStream();

        SampleIndexGtEntryBuilderWithPartialBuilds(String gt) {
            this(gt, 100);
        }

        SampleIndexGtEntryBuilderWithPartialBuilds(String gt, int threshold) {
            super(gt);
            this.threshold = threshold;
        }

        @Override
        public boolean add(SampleVariantIndexEntry variantIndexEntry) {
            boolean add = super.add(variantIndexEntry);
            if (entries.size() >= threshold) {
                partialBuild();
            }

            return add;
        }

        @Override
        public boolean containsVariant(SampleVariantIndexEntry variantIndexEntry) {
            return containsVariant(variantIndexEntry.getVariant());
        }

        public boolean containsVariant(Variant variant) {
            for (Variant v : variants) {
                if (v.sameGenomicVariant(variant)) {
                    return true;
                }
            }
            for (SampleVariantIndexEntry entry : entries) {
                if (entry.getVariant().sameGenomicVariant(variant)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int containsVariants(SampleIndexGtEntryBuilder other) {
            // Build a temporary TreeSet for fast searching.
            TreeSet<Variant> set = new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR);
            set.addAll(variants);
            for (SampleVariantIndexEntry entry : entries) {
                set.add(entry.getVariant());
            }

            int c = 0;
            if (other instanceof SampleIndexGtEntryBuilderWithPartialBuilds) {
                for (Variant otherVariant : ((SampleIndexGtEntryBuilderWithPartialBuilds) other).variants) {
                    if (set.contains(otherVariant)) {
                        c++;
                    }
                }
            }
            for (SampleVariantIndexEntry otherEntry : other.getEntries()) {
                if (set.contains(otherEntry.getVariant())) {
                    c++;
                }
            }
            return c;
        }

        private void partialBuild() {
            BitBuffer fileIndexBuffer = new BitBuffer(fileIndex.getBitsLength() * entries.size());
            int offset = 0;

            variants.ensureCapacity(variants.size() + entries.size());
            for (SampleVariantIndexEntry gtEntry : entries) {
                Variant variant = gtEntry.getVariant();
                if (prev == null || !prev.getVariant().sameGenomicVariant(variant)) {
                    variants.add(variant);
                } else {
                    // Mark previous variant as MultiFile
                    fileIndex.setMultiFile(fileIndexBuffer, offset - fileIndex.getBitsLength());
                }
                fileIndexBuffer.setBitBuffer(gtEntry.getFileIndex(), offset);
                offset += fileIndex.getBitsLength();
                prev = gtEntry;
            }
            this.fileIndexBuffer.write(fileIndexBuffer);

            entries.clear();
        }

        @Override
        public void build(Put put) {
            partialBuild();

            byte[] variantsBuffer = variantConverter.toBytes(variants);
            int variantsCount = variants.size();

            // Add to put
            put.addColumn(COLUMN_FAMILY, SampleIndexSchema.toGenotypeColumn(gt), variantsBuffer);
            put.addColumn(COLUMN_FAMILY, SampleIndexSchema.toGenotypeCountColumn(gt), Bytes.toBytes(variantsCount));
            put.addColumn(COLUMN_FAMILY, SampleIndexSchema.toFileIndexColumn(gt), fileIndexBuffer.toByteArray());
        }
    }



}
