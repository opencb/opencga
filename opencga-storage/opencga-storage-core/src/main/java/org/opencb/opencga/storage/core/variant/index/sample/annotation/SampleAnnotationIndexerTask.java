package org.opencb.opencga.storage.core.variant.index.sample.annotation;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleGenotypeIndexerTask;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntryChunk;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariantAnnotation;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.util.*;

public class SampleAnnotationIndexerTask implements Task<Variant, SampleIndexEntry> {

    private final SampleIndexVariantAnnotationConverter annotationConverter;
    private final List<Integer> sampleIds;
    private final SampleIndexSchema schema;
    private final Map<SampleIndexEntryChunk, Chunk> chunks;

    public SampleAnnotationIndexerTask(List<Integer> sampleIds, SampleIndexSchema schema) {
        this.annotationConverter = new SampleIndexVariantAnnotationConverter(schema);
        this.sampleIds = sampleIds;
        this.schema = schema;
        chunks = new LinkedHashMap<>();
    }


    @Override
    public List<SampleIndexEntry> apply(List<Variant> variants) throws Exception {
        for (Variant variant : variants) {
            if (!SampleGenotypeIndexerTask.validVariant(variant)) {
                continue;
            }
            SampleIndexVariantAnnotation annotation = annotationConverter.convert(variant.getAnnotation());
            Chunk chunk = chunks.computeIfAbsent(new SampleIndexEntryChunk(variant), Chunk::new);
            for (int i = 0; i < sampleIds.size(); i++) {
//                Integer sampleId = sampleIds.get(i);
                SampleEntry sampleEntry = variant.getStudies().get(0).getSamples().get(i);
                String gt = sampleEntry.getData().get(0);

                boolean validGt;
                if (gt == null || gt.isEmpty()) {
                    gt = GenotypeClass.NA_GT_VALUE;
                    validGt = true;
                } else {
                    validGt = SampleIndexSchema.isAnnotatedGenotype(gt);
                }
                if (validGt) {
                    chunk.addVariant(i, gt, annotation);
                }
            }
        }

        return drain(3);
    }

    @Override
    public List<SampleIndexEntry> drain() throws Exception {
        return drain(0);
    }

    private List<SampleIndexEntry> drain(int buffer) {
        List<SampleIndexEntry> result = null;
        while (chunks.size() > buffer) {
            Iterator<Chunk> iterator = chunks.values().iterator();
            Chunk chunk = iterator.next();
            iterator.remove();
            if (result == null) {
                result = chunk.toEntries();
            } else {
                result.addAll(chunk.toEntries());
            }
        }
        if (result != null) {
            return result;
        } else {
            return Collections.emptyList();
        }
    }


    protected class Chunk {
        private final List<Map<String, SampleIndexVariantAnnotationBuilder>> builders;
        private final SampleIndexEntryChunk indexChunk;

        Chunk(SampleIndexEntryChunk indexChunk) {
            this.indexChunk = indexChunk;
            builders = new ArrayList<>(sampleIds.size());
            for (int i = 0; i < sampleIds.size(); i++) {
                builders.add(new HashMap<>());
            }
        }

        public Map<String, SampleIndexVariantAnnotationBuilder> get(int sampleIdx) {
            return builders.get(sampleIdx);
        }

        public SampleIndexVariantAnnotationBuilder get(int sampleIdx, String gt) {
            return builders.get(sampleIdx).computeIfAbsent(gt, k -> new SampleIndexVariantAnnotationBuilder(schema));
        }

        public void addVariant(int sampleIdx, String gt, SampleIndexVariantAnnotation annotation) {
            get(sampleIdx, gt).add(annotation);
        }

        private List<SampleIndexEntry> toEntries() {
            int sampleIdx = 0;
            List<SampleIndexEntry> entries = new ArrayList<>(builders.size());
            for (Map<String, SampleIndexVariantAnnotationBuilder> sampleBuilder : builders) {
                Integer sampleId = sampleIds.get(sampleIdx);

                SampleIndexEntry sampleIndexEntry = new SampleIndexEntry(sampleId, indexChunk.getChromosome(), indexChunk.getBatchStart());
                for (Map.Entry<String, SampleIndexVariantAnnotationBuilder> builderEntry : sampleBuilder.entrySet()) {
                    String gt = builderEntry.getKey();
                    SampleIndexVariantAnnotationBuilder builder = builderEntry.getValue();
                    if (!builder.isEmpty()) {
                        sampleIndexEntry.addGtEntry(builder.buildAndReset(gt));
                    }
                }
                if (!sampleIndexEntry.getGts().isEmpty()) {
                    entries.add(sampleIndexEntry);
                }
                sampleIdx++;
            }
            return entries;
        }

    }


}
