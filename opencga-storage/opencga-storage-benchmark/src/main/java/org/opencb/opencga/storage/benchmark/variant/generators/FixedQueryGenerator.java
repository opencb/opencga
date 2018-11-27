package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQueries;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQuery;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by wasim on 30/10/18.
 */
public class FixedQueryGenerator extends QueryGenerator {

    public static final String FIXED_QUERY = "fixed-query";
    public static final String FIXED_QUERIES_FILE = "fixedQueries.yml";
    private String queryId;
    private FixedQuery fixedQuery;
    private FixedQueries fixedQueries;

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);

        Path queryFilePath;
        String queryFile = params.get(FILE);
        queryId = params.get(FIXED_QUERY);

        if (queryFile == null || queryFile.isEmpty()) {
            queryFile = params.get(DATA_DIR).concat("/").concat(FIXED_QUERIES_FILE);
        }

        queryFilePath = Paths.get(queryFile);
        fixedQueries = readYmlFile(queryFilePath, FixedQueries.class);

        for (FixedQuery fixedQuery : fixedQueries.getQueries()) {
            if (queryId.equals(fixedQuery.getId())) {
                this.fixedQuery = fixedQuery;
                break;
            }
        }
    }

    @Override
    public Query generateQuery(Query query) {
        query.putAll(fixedQueries.getBaseQuery());
        query.putAll(fixedQuery.getQuery());
        appendRandomSessionId(fixedQueries.getSessionIds(), query);
        return query;
    }

    @Override
    public String getQueryId() {
        return queryId;
    }
}
