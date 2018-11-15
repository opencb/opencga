package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQueries;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wasim on 30/10/18.
 */
public class FixedQueryGenerator extends QueryGenerator {

    public static final String FIXED_QUERY = "fixed-query";
    public static final String FIXED_QUERIES_FILE = "fixedQueries.yml";
    private FixedQueries fixedQueries;
    private List<String> queries;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private AtomicInteger counter = new AtomicInteger(0);
    private String queryId;

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);

        String query = params.get(FIXED_QUERY);
        if (!query.isEmpty()) {
            queries = Arrays.asList(query.split(","));
        }

        Path queryFilePath;
        String queryFile = params.get(FILE);

        if (queryFile == null || queryFile.isEmpty()) {
            queryFile = params.get(DATA_DIR).concat("/").concat(FIXED_QUERIES_FILE);
        }

        queryFilePath = Paths.get(queryFile);
        fixedQueries = readFixedQueriesFromFile(queryFilePath, queries);
    }

    @Override
    public Query generateQuery(Query query) {
        FixedQuery fixedQuery = fixedQueries.getQueries().get(counter.getAndIncrement());
        query.putAll(fixedQuery.getParams());
        appendRandomSessionId(fixedQueries.getSessionIds(), query);
        this.queryId = fixedQuery.getId();
        counter.compareAndSet(fixedQueries.getQueries().size(), 0);
        return query;
    }


    protected FixedQueries readFixedQueriesFromFile(Path path, List<String> filterQueries) {
        FixedQueries fixedQueries = readYmlFile(path, FixedQueries.class);
        FixedQueries result = new FixedQueries();
        if (filterQueries != null && !filterQueries.get(0).equals("all")) {
            for (FixedQuery fixedQuery : fixedQueries.getQueries()) {
                for (String query : filterQueries) {
                    if (fixedQuery.getId().equals(query)) {
                        result.addQuery(fixedQuery);
                    }
                }
            }
            result.setSessionIds(fixedQueries.getSessionIds());
        } else {
            result = fixedQueries;
        }
        return result;
    }

    @Override
    public String getQueryId() {
        return queryId;
    }
}
