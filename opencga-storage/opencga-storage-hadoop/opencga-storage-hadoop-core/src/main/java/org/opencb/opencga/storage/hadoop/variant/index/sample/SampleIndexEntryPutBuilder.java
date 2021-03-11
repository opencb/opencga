package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.toRowKey;

public class SampleIndexEntryPutBuilder {

    private final int sampleId;
    private final String chromosome;
    private final int position;

    private final Map<String, SampleIndexGtEntryBuilder> gts;
    private final SampleIndexVariantBiConverter variantConverter;
    private final byte[] family = GenomeHelper.COLUMN_FAMILY_BYTES;
    private SampleIndexSchema schema;
    private FileIndexSchema fileIndex;
    private SampleVariantIndexEntry.SampleVariantIndexEntryComparator comparator;

    public SampleIndexEntryPutBuilder(int sampleId, Variant variant, SampleIndexSchema schema) {
        this(sampleId, variant.getChromosome(), SampleIndexSchema.getChunkStart(variant.getStart()), schema);
    }

    public SampleIndexEntryPutBuilder(int sampleId, String chromosome, int position, SampleIndexSchema schema) {
        this.sampleId = sampleId;
        this.chromosome = chromosome;
        this.position = position;
        gts = new HashMap<>();
        variantConverter = new SampleIndexVariantBiConverter(schema);
        this.schema = schema;
        fileIndex = this.schema.getFileIndex();
        comparator = new SampleVariantIndexEntry.SampleVariantIndexEntryComparator(schema);
    }

    public SampleIndexEntryPutBuilder(int sampleId, String chromosome, int position, SampleIndexSchema schema,
                                      Map<String, TreeSet<SampleVariantIndexEntry>> map) {
        this(sampleId, chromosome, position, schema);
        for (Map.Entry<String, TreeSet<SampleVariantIndexEntry>> entry : map.entrySet()) {
            gts.put(entry.getKey(), new SampleIndexGtEntryBuilder(entry.getKey(), entry.getValue()));
        }
    }

    public SampleIndexEntryPutBuilder add(String gt, SampleVariantIndexEntry variantIndexEntry) {
        get(gt).add(variantIndexEntry);
        return this;
    }

    private SampleIndexGtEntryBuilder get(String gt) {
        return gts.computeIfAbsent(gt, gt1 -> new SampleIndexGtEntryBuilder(gt1, comparator));
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
            SortedSet<SampleVariantIndexEntry> gtEntries = gtBuilder.getEntries();
            String gt = gtBuilder.getGt();

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

            put.addColumn(family, SampleIndexSchema.toGenotypeColumn(gt), variantsBytes);
            put.addColumn(family, SampleIndexSchema.toGenotypeCountColumn(gt), Bytes.toBytes(variants.size()));
            put.addColumn(family, SampleIndexSchema.toFileIndexColumn(gt), fileIndexBuffer.getBuffer());
        }
        int discrepancies = 0;

        Iterator<SampleIndexGtEntryBuilder> iterator = gts.values().iterator();
        while (iterator.hasNext()) {
            SampleIndexGtEntryBuilder gt = iterator.next();
            iterator.remove();
            for (SampleIndexGtEntryBuilder otherGt : gts.values()) {
                for (SampleVariantIndexEntry entry : gt.entries) {
                    if (otherGt.containsVariant(entry)) {
                        discrepancies++;
                    }
                }
            }
        }
        put.addColumn(family, SampleIndexSchema.toGenotypeDiscrepanciesCountColumn(), Bytes.toBytes(discrepancies));

        return put;
    }

    private static class SampleIndexGtEntryBuilder {
        private final String gt;
        private final TreeSet<SampleVariantIndexEntry> entries;

        SampleIndexGtEntryBuilder(String gt, Comparator<? super SampleVariantIndexEntry> comparator) {
            this.gt = gt;
            entries = new TreeSet<>(comparator);
        }

        SampleIndexGtEntryBuilder(String gt, TreeSet<SampleVariantIndexEntry> entries) {
            this.gt = gt;
            this.entries = entries;
        }

        public String getGt() {
            return gt;
        }

        public SortedSet<SampleVariantIndexEntry> getEntries() {
            return entries;
        }

        public boolean add(SampleVariantIndexEntry variantIndexEntry) {
            return entries.add(variantIndexEntry);
        }

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
    }




}
