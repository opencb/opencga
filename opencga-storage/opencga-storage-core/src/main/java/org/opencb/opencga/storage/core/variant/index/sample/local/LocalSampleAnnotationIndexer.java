package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleAnnotationIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleIndexVariantAnnotationBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleIndexVariantAnnotationConverter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariantAnnotation;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalSampleAnnotationIndexer extends SampleAnnotationIndexer {

    private final VariantStorageEngine engine;
    private final LocalSampleIndexDBAdaptor localAdaptor;

    public LocalSampleAnnotationIndexer(LocalSampleIndexDBAdaptor sampleIndexDBAdaptor,
                                                 VariantStorageEngine engine) {
        super(sampleIndexDBAdaptor);
        this.engine = engine;
        this.localAdaptor = sampleIndexDBAdaptor;
    }

    @Override
    protected void runBatch(int studyId, List<Integer> samples, int sampleIndexVersion, ObjectMap options)
            throws StorageEngineException {
        SampleIndexSchema schema = new SampleIndexSchema(localAdaptor.getMetadataManager().getStudyMetadata(studyId)
                .getSampleIndexConfiguration(sampleIndexVersion).getConfiguration(), sampleIndexVersion);

        SampleIndexVariantAnnotationConverter annotationConverter = new SampleIndexVariantAnnotationConverter(schema);

        // Get list of actual existing regions from filenames
        List<Region> regions = localAdaptor.getRegionBounds(studyId, sampleIndexVersion, samples);

        // Create progress logger
        ProgressLogger progressLogger = new ProgressLogger("Annotating regions for " + samples.size() + " samples",
                regions.size());

        // Reuse builders across regions since they are reset after each use
        Map<Integer, Map<String, SampleIndexVariantAnnotationBuilder>> sampleBuilders = new HashMap<>();

        // Process one region at a time
        for (Region region : regions) {
            String chromosome = region.getChromosome();
            int batchStart = region.getStart();

            // Read sample entries for this region
            Map<Integer, SampleIndexEntry> sampleEntries = new HashMap<>();
            for (Integer sampleId : samples) {
                SampleIndexEntry entry = localAdaptor.readEntry(studyId, sampleIndexVersion, sampleId,
                        chromosome, batchStart);
                if (entry != null) {
                    sampleEntries.put(sampleId, entry);
                }
            }

            if (sampleEntries.isEmpty()) {
                progressLogger.increment(1);
                continue;
            }

            // Query variants for this region (sorted)
            Query query = new Query(VariantQueryParam.STUDY.key(), studyId)
                    .append(VariantQueryParam.REGION.key(), region);

            QueryOptions queryOptions = new QueryOptions()
                    .append(QueryOptions.INCLUDE, VariantField.ANNOTATION)
                    .append(QueryOptions.SORT, true)
                    .append("quiet", true);

            try (VariantDBIterator dbIt = engine.getDBAdaptor().iterator(query, queryOptions)) {
                while (dbIt.hasNext()) {
                    Variant annotatedVariant = dbIt.next();
                    String variantKey = annotatedVariant.toString();

                    SampleIndexVariantAnnotation newAnnotation = annotationConverter
                            .convert(annotatedVariant.getAnnotation());

                    // Update each sample's builders
                    for (Integer sampleId : sampleEntries.keySet()) {
                        SampleIndexEntry entry = sampleEntries.get(sampleId);

                        // Get or create builders for this sample
                        Map<String, SampleIndexVariantAnnotationBuilder> gtBuilders = sampleBuilders
                                .computeIfAbsent(sampleId, k -> new HashMap<>());

                        // Process each genotype
                        for (String gt : entry.getGts().keySet()) {
                            SampleIndexVariantAnnotationBuilder builder = gtBuilders.computeIfAbsent(gt,
                                    k -> new SampleIndexVariantAnnotationBuilder(schema));

                            builder.add(newAnnotation);
                        }
                    }
                }
            } catch (Exception e) {
                throw new StorageEngineException("Error querying variants for region " + region, e);
            }

            // Now build and update all entries for this region (write once per sample per
            // region)
            for (Integer sampleId : sampleEntries.keySet()) {
                SampleIndexEntry entry = sampleEntries.get(sampleId);
                Map<String, SampleIndexVariantAnnotationBuilder> gtBuilders = sampleBuilders.get(sampleId);

                if (gtBuilders != null) {
                    for (Map.Entry<String, SampleIndexVariantAnnotationBuilder> builderEntry : gtBuilders.entrySet()) {
                        String gt = builderEntry.getKey();
                        SampleIndexVariantAnnotationBuilder builder = builderEntry.getValue();

                        if (!builder.isEmpty()) {
                            SampleIndexEntry.SampleIndexGtEntry gtEntry = entry.getGtEntry(gt);
                            // Use the new buildAndReset method that updates the gtEntry directly
                            builder.buildAndReset(gtEntry);
                        }
                    }

                    // Write updated entry once after processing all variants in the region
                    localAdaptor.writeEntry(studyId, sampleIndexVersion, entry);
                }
            }

            // Increment progress
            progressLogger.increment(1, () -> " [" + region + "]");
        }
    }
}
