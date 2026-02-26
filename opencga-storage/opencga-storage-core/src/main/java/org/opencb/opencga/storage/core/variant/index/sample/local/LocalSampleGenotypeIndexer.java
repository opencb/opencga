package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleGenotypeIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexVariantConverter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalSampleGenotypeIndexer extends SampleGenotypeIndexer {
    private static final int INCLUDE_SAMPLES_THRESHOLD = 50;

    private final VariantStorageEngine engine;
    private final LocalSampleIndexDBAdaptor localAdaptor;

    public LocalSampleGenotypeIndexer(LocalSampleIndexDBAdaptor sampleIndexDBAdaptor,
            VariantStorageEngine engine) {
        super(sampleIndexDBAdaptor);
        this.engine = engine;
        this.localAdaptor = sampleIndexDBAdaptor;
    }

    @Override
    protected void indexBatch(int studyId, SampleIndexSchema schema, List<Integer> sampleIds, ObjectMap options)
            throws StorageEngineException {
        SampleIndexVariantConverter variantConverter = new SampleIndexVariantConverter(schema);

        // Convert sample IDs to sample names
        List<String> sampleNames = new ArrayList<>(sampleIds.size());
        for (Integer sampleId : sampleIds) {
            sampleNames.add(metadataManager.getSampleName(studyId, sampleId));
        }

        VariantQuery query = new VariantQuery()
                .study(metadataManager.getStudyName(studyId));

        // Use includeSamples modifier if too many samples, otherwise use samples filter
        if (sampleIds.size() > INCLUDE_SAMPLES_THRESHOLD) {
            query.includeSample(sampleNames);
        } else {
            query.sample(sampleNames);
        }

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(VariantField.ID, VariantField.STUDIES));

        try (VariantDBIterator iterator = engine.getDBAdaptor().iterator(query, queryOptions)) {
            Map<Integer, SampleIndexEntryBuilder> builders = new HashMap<>();
            int currentBatchStart = -1;
            String currentChromosome = null;

            while (iterator.hasNext()) {
                Variant variant = iterator.next();
                int batchStart = SampleIndexSchema.getChunkStart(variant.getStart());

                if (!variant.getChromosome().equals(currentChromosome) || batchStart != currentBatchStart) {
                    flush(studyId, schema, builders);
                    builders.clear();
                    currentChromosome = variant.getChromosome();
                    currentBatchStart = batchStart;
                }

                StudyEntry studyEntry = variant.getStudies().get(0);
                for (int i = 0; i < sampleIds.size(); i++) {
                    Integer sampleId = sampleIds.get(i);
                    String sampleName = sampleNames.get(i);

                    // Get genotype by sample name, use NA if not present
                    String gt = studyEntry.getSampleData(sampleName, "GT");
                    if (gt == null) {
                        gt = GenotypeClass.NA_GT_VALUE;
                    }

                    if (SampleIndexSchema.validGenotype(gt)) {
                        SampleIndexVariant sampleIndexVariant = variantConverter.createSampleIndexVariant(0, 0,
                                variant, studyEntry.getSampleData(i));
                        SampleIndexEntryBuilder builder = builders.computeIfAbsent(sampleId,
                                k -> new SampleIndexEntryBuilder(k, variant.getChromosome(), batchStart, schema, true,
                                        false));
                        builder.add(gt, sampleIndexVariant);
                    }
                }
            }
            flush(studyId, schema, builders);
        } catch (StorageEngineException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageEngineException("Error constructing sample index for study " + studyId, e);
        }
    }

    private void flush(int studyId, SampleIndexSchema schema, Map<Integer, SampleIndexEntryBuilder> builders)
            throws StorageEngineException {
        if (builders.isEmpty()) {
            return;
        }
        for (SampleIndexEntryBuilder builder : builders.values()) {
            localAdaptor.writeEntry(studyId, schema.getVersion(), builder.buildEntry());
        }
    }
}
