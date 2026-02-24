package org.opencb.opencga.storage.mongodb.variant.index.sample;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.index.sample.query.LocusQuery;
import org.opencb.opencga.storage.core.variant.index.sample.query.SingleSampleIndexQuery;

import java.util.Map;

public final class MongoDBSampleIndexQueryBuilder {

    private final VariantStorageMetadataManager metadataManager;

    public MongoDBSampleIndexQueryBuilder(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public Bson buildFilter(SingleSampleIndexQuery query, LocusQuery locusQuery) {
        Document filter = new Document();
        int studyId = resolveStudyId(query.getStudy());
        int sampleId = metadataManager.getSampleId(studyId, query.getSample());
        if (locusQuery != null && locusQuery.getChunkRegion() != null) {
            String documentIdPrefix = DocumentToSampleIndexEntryConverter.buildDocumentId(sampleId,
                    locusQuery.getChunkRegion().getChromosome(),
                    locusQuery.getChunkRegion().getStart());
            // Use _id range for tighter filtering when possible
            filter.put("_id", new Document("$gte", documentIdPrefix)
                    .append("$lte", DocumentToSampleIndexEntryConverter.buildDocumentId(sampleId,
                            locusQuery.getChunkRegion().getChromosome(), locusQuery.getChunkRegion().getEnd())));
        } else {
            filter.put(DocumentToSampleIndexEntryConverter.SAMPLE_ID, sampleId);
        }
        // Additional filters mostly handled in-memory; placeholder left for future expansion
        return filter;
    }

    public Bson buildProjection(SingleSampleIndexQuery query, boolean includeAllFields) {
        if (includeAllFields) {
            // Return null so MongoDB includes every field, ensuring gt_* entries remain visible.
            return null;
        }
        Document projection = new Document();
        projection.put("_id", 1);
        projection.put(DocumentToSampleIndexEntryConverter.SAMPLE_ID, 1);
        projection.put(DocumentToSampleIndexEntryConverter.CHROMOSOME, 1);
        projection.put(DocumentToSampleIndexEntryConverter.BATCH_START, 1);
        projection.put(DocumentToSampleIndexEntryConverter.DISCREPANCIES, 1);
        projection.put(DocumentToSampleIndexEntryConverter.MENDELIAN, 1);
        projection.put(DocumentToSampleIndexEntryConverter.GT_LIST, 1);
        for (String genotype : query.getGenotypes()) {
            for (String gtField : DocumentToSampleIndexEntryConverter.GT_FIELDS) {
                projection.put(DocumentToSampleIndexEntryConverter.getGenotypeField(genotype, gtField), 1);
            }
        }
        return new Document(projection);
    }

    public Document defaultSort() {
        return new Document("_id", 1);
    }

    private int resolveStudyId(Object study) {
        if (study == null || study instanceof String && ((String) study).isEmpty()) {
            Map<String, Integer> studies = metadataManager.getStudies(null);
            if (studies.size() == 1) {
                return studies.values().iterator().next();
            } else {
                throw VariantQueryException.missingStudy(studies.keySet());
            }
        } else {
            return metadataManager.getStudyId(study);
        }
    }
}
