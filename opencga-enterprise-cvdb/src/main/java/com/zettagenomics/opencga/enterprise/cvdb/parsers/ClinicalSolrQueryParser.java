package com.zettagenomics.opencga.enterprise.cvdb.parsers;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser;

import java.util.List;

public class ClinicalSolrQueryParser extends SolrQueryParser {

    public ClinicalSolrQueryParser(SearchIndexMetadata indexMetadata) {
        super(indexMetadata);
    }

    @Override
    protected List<String> parseStudyNames(List<String> studiesNames) {
        // TODO: Implement this method
        throw new UnsupportedOperationException("FIXME");
    }

    @Override
    protected String getDefaultStudyName(Query query) {
        // TODO: Implement this method
        throw new UnsupportedOperationException("FIXME");
    }
}
