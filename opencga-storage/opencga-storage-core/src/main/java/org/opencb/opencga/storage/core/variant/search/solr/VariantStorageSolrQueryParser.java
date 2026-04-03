package org.opencb.opencga.storage.core.variant.search.solr;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter.studyIdToSearchModel;

public class VariantStorageSolrQueryParser extends SolrQueryParser {
    protected final VariantStorageMetadataManager variantStorageMetadataManager;

    public static VariantStorageSolrQueryParser create(VariantStorageMetadataManager variantStorageMetadataManager,
            SearchIndexMetadata indexMetadata) {
        if (VariantSearchManager.isStatsFunctionalQueryEnabled(indexMetadata)) {
            return new FunctionalStatsVariantStorageSolrQueryParser(variantStorageMetadataManager, indexMetadata);
        } else {
            return new VariantStorageSolrQueryParser(variantStorageMetadataManager, indexMetadata);
        }
    }

    protected VariantStorageSolrQueryParser(VariantStorageMetadataManager variantStorageMetadataManager,
                                            SearchIndexMetadata indexMetadata) {
        super(indexMetadata);
        this.variantStorageMetadataManager = variantStorageMetadataManager;
    }

    @Override
    protected List<String> parseStudyNames(List<String> studiesNames) {
        Map<String, Integer> studies = variantStorageMetadataManager.getStudies(null);
        // Build reverse map: id -> name
        Map<Integer, String> idToName = new HashMap<>(studies.size());
        studies.forEach((name, id) -> idToName.put(id, name));

        List<String> result = new ArrayList<>(studiesNames.size());
        for (String studyName : studiesNames) {
            boolean negated = isNegated(studyName);
            String rawName = negated ? removeNegation(studyName) : studyName;
            Integer studyId = variantStorageMetadataManager.getStudyId(rawName, false, studies);
            if (studyId != null) {
                String resolved = studyIdToSearchModel(idToName.get(studyId));
                result.add(negated ? NOT + resolved : resolved);
            }
        }
        return result;
    }

    @Override
    protected void parseVariantStatsFilter(VariantQueryParam param, String value, FreqField field, FreqType type,
                                           String study, String cohort, String op, String numValue,
                                           boolean addOr, List<String> filters, List<String> auxFilters) {
        // Resolve study ID/name to canonical study name
        int studyId = variantStorageMetadataManager.getStudyId(study);
        String resolvedStudy = variantStorageMetadataManager.getStudyName(studyId);

        // Validate cohort exists
        Integer cohortId = variantStorageMetadataManager.getCohortId(studyId, cohort);
        if (cohortId == null) {
            throw VariantQueryException.cohortNotFound(cohort, studyId, variantStorageMetadataManager);
        }

        super.parseVariantStatsFilter(param, value, field, type, resolvedStudy, cohort, op, numValue, addOr, filters, auxFilters);
    }

    @Override
    protected String getDefaultStudyName(Query query) {
        StudyMetadata defaultStudy = VariantQueryParser.getDefaultStudy(query, variantStorageMetadataManager);
        String defaultStudyName = (defaultStudy == null)
                ? null
                : defaultStudy.getName();
        return defaultStudyName;
    }
}
