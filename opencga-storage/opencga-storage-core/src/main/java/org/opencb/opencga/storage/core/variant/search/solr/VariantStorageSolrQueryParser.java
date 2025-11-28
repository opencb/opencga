package org.opencb.opencga.storage.core.variant.search.solr;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;

import java.util.*;

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
        Set<Integer> studyIds = new HashSet<>(variantStorageMetadataManager.getStudyIds(studiesNames));
        List<String> studyNames = new ArrayList<>(studyIds.size());
        Map<String, Integer> map = variantStorageMetadataManager.getStudies(null);
        map.forEach((name, id) -> {
            if (studyIds.contains(id)) {
                studyNames.add(studyIdToSearchModel(name));
            }
        });
        return studyNames;
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
