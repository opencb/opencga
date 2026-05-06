package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

public class MongoDBVariantQueryParser extends VariantQueryParser {

    public MongoDBVariantQueryParser(CellBaseUtils cellBaseUtils, VariantStorageMetadataManager metadataManager) {
        super(cellBaseUtils, metadataManager);
    }

    @Override
    public Query preProcessQuery(Query originalQuery, QueryOptions options) {
        Query query = super.preProcessQuery(originalQuery, options);
        VariantQueryUtils.convertGenesToRegionsQuery(query, cellBaseUtils);
        return query;
    }
}
