package org.opencb.opencga.storage.core.variant.index.sample.annotation;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.IssueEntry;
import org.opencb.biodata.models.variant.avro.IssueType;
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
            StudyEntry studyEntry = variant.getStudies().get(0);
            List<IssueEntry> issues = studyEntry.getIssues();
            boolean hasIssues = !issues.isEmpty();
            // For MULTI-file samples, sample names are needed to match DISCREPANCY IssueEntries to sample indices.
            List<String> sampleNames = hasIssues ? studyEntry.getOrderedSamplesName() : null;
            for (int i = 0; i < sampleIds.size(); i++) {
                SampleEntry sampleEntry = studyEntry.getSamples().get(i);
                String gt = sampleEntry.getData().get(0);

                boolean validGt;
                if (gt == null || gt.isEmpty()) {
                    gt = GenotypeClass.NA_GT_VALUE;
                    validGt = true;
                } else {
                    validGt = SampleIndexSchema.isAnnotatedGenotype(gt);
                }

                if (validGt) {
                    chunk.addVariant(i, gt, variant, annotation);
                }
                if (hasIssues) {
                    // For MULTI-file samples, also add annotation entries for secondary file entries (DISCREPANCY IssueEntries).
                    // Each DISCREPANCY issue corresponds to an extra sample index variant entry added by a separate file load;
                    // the annotation index must have a matching byte for each such entry.
                    String sampleName = sampleNames.get(i);
                    for (IssueEntry issue : issues) {
                        if (IssueType.DISCREPANCY == issue.getType()
                                && sampleName.equals(issue.getSample().getSampleId())) {
                            String issueGt = issue.getSample().getData().get(0);
                            boolean issueValidGt;
                            if (issueGt == null || issueGt.isEmpty()) {
                                issueGt = GenotypeClass.NA_GT_VALUE;
                                issueValidGt = true;
                            } else {
                                issueValidGt = SampleIndexSchema.isAnnotatedGenotype(issueGt);
                            }
                            if (issueValidGt) {
                                chunk.addVariant(i, issueGt, variant, annotation);
                            }
                        }
                    }
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
        /**
         * Per-sample, per-GT buffer of (variant, annotation) pairs.
         * Variants are sorted by {@link SampleIndexSchema#INTRA_CHROMOSOME_VARIANT_COMPARATOR} at flush time
         * to ensure positional alignment with the genotype index, which stores variants in that same order.
         */
        private final List<Map<String, List<VariantAnnotationPair>>> builders;
        private final SampleIndexEntryChunk indexChunk;

        Chunk(SampleIndexEntryChunk indexChunk) {
            this.indexChunk = indexChunk;
            builders = new ArrayList<>(sampleIds.size());
            for (int i = 0; i < sampleIds.size(); i++) {
                builders.add(new HashMap<>());
            }
        }

        public void addVariant(int sampleIdx, String gt, Variant variant, SampleIndexVariantAnnotation annotation) {
            builders.get(sampleIdx)
                    .computeIfAbsent(gt, k -> new ArrayList<>())
                    .add(new VariantAnnotationPair(variant, annotation));
        }

        private List<SampleIndexEntry> toEntries() {
            int sampleIdx = 0;
            List<SampleIndexEntry> entries = new ArrayList<>(builders.size());
            for (Map<String, List<VariantAnnotationPair>> sampleBuilderMap : builders) {
                Integer sampleId = sampleIds.get(sampleIdx);

                SampleIndexEntry sampleIndexEntry = new SampleIndexEntry(sampleId, indexChunk.getChromosome(), indexChunk.getBatchStart());
                for (Map.Entry<String, List<VariantAnnotationPair>> builderEntry : sampleBuilderMap.entrySet()) {
                    String gt = builderEntry.getKey();
                    List<VariantAnnotationPair> pairs = builderEntry.getValue();
                    // Sort to match genotype-index order before writing annotation bytes.
                    pairs.sort((a, b) -> SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR.compare(a.getVariant(), b.getVariant()));
                    SampleIndexVariantAnnotationBuilder builder = new SampleIndexVariantAnnotationBuilder(schema, pairs.size());
                    for (VariantAnnotationPair pair : pairs) {
                        builder.add(pair.getAnnotation());
                    }
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

    private static final class VariantAnnotationPair {
        private final Variant variant;
        private final SampleIndexVariantAnnotation annotation;

        VariantAnnotationPair(Variant variant, SampleIndexVariantAnnotation annotation) {
            this.variant = variant;
            this.annotation = annotation;
        }

        Variant getVariant() {
            return variant;
        }

        SampleIndexVariantAnnotation getAnnotation() {
            return annotation;
        }
    }

}
