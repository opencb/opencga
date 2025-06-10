package org.opencb.opencga.storage.core.variant.search;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VariantToSolrBeanConverterTask implements Converter<Variant, Pair<SolrInputDocument, SolrInputDocument>> {

    private final VariantSearchToVariantConverter converter;
    private final DocumentObjectBinder binder;
    private final VariantSecondaryIndexFilter filter;
    private final boolean statsCollectionEnabled;

    public VariantToSolrBeanConverterTask(DocumentObjectBinder binder, VariantStorageMetadataManager metadataManager, boolean statsCollectionEnabled) {
        this.binder = binder;
        this.filter = new VariantSecondaryIndexFilter(metadataManager.getStudies());
        this.statsCollectionEnabled = statsCollectionEnabled;
        HashMap<String, Integer> cohorts = new HashMap<>();
        for (Map.Entry<String, Integer> entry : metadataManager.getStudies().entrySet()) {
            String studyName = entry.getKey();
            studyName = VariantSearchToVariantConverter.studyIdToSearchModel(studyName);

            for (CohortMetadata cohort : metadataManager.getCalculatedOrPartialCohorts(entry.getValue())) {
                String cohortName = cohort.getName();
                cohorts.put(studyName + VariantSearchUtils.FIELD_SEPARATOR + cohortName, cohort.getId());
            }
        }
        this.converter = new VariantSearchToVariantConverter(cohorts);
    }

    @Override
    public List<Pair<SolrInputDocument, SolrInputDocument>> apply(List<Variant> from) {
        List<Pair<SolrInputDocument, SolrInputDocument>> convertedBatch = new ArrayList<>(from.size());

        for (Variant item : from) {
            Pair<SolrInputDocument, SolrInputDocument> convert = this.convert(item);
            if (convert != null) {
                convertedBatch.add(convert);
            }
        }

        return convertedBatch;
    }

    @Override
    public Pair<SolrInputDocument, SolrInputDocument> convert(Variant variant) {
        VariantStorageEngine.SyncStatus status = filter.getSyncStatus(variant);
        switch (status) {
            case SYNCHRONIZED:
                // Nothing to sync!
                return null;
            case NOT_SYNCHRONIZED:
            case STATS_NOT_SYNC: {
                final SolrInputDocument mainDocument;
                final SolrInputDocument statsDocument;

                if (statsCollectionEnabled) {
                    if (status == VariantStorageEngine.SyncStatus.NOT_SYNCHRONIZED) {
                        // Need to sync main collection.
                        VariantSearchModel variantSearchModelAnnotation = converter.convertToStorageType(variant, false, true);
                        mainDocument = binder.toSolrInputDocument(variantSearchModelAnnotation);
                    } else {
                        // Main document not needed. Only stats are out of sync
                        mainDocument = null;
                    }

                    VariantSearchModel variantSearchModelStats = converter.convertToStorageType(variant, true, false);
                    statsDocument = binder.toSolrInputDocument(variantSearchModelStats);
                } else {
                    // TODO: If sync status is STATS_NOT_SYNC, we should generate only the stats document
                    // No stats collection enabled, so we only convert the main document that contains everything
                    VariantSearchModel variantSearchModel = converter.convertToStorageType(variant);
                    mainDocument = binder.toSolrInputDocument(variantSearchModel);
                    statsDocument = null;
                }
                return Pair.of(mainDocument,
                        statsDocument);
            }
            case STATS_NOT_SYNC_AND_STUDIES_UNKNOWN:
            case STUDIES_UNKNOWN_SYNC:
                // No uncertainty is expected at this point, so we should not reach here.
            default:
                throw new IllegalStateException("Unexpected value: " + filter.getSyncStatus(variant));
        }
    }

}
