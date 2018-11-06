package org.opencb.opencga.storage.benchmark.variant.generators;

import com.google.common.base.Throwables;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQueries;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 30/10/18.
 */
public class FixedQueryGenerator extends QueryGenerator {

    public static final String FIXED_QUERY = "fixed-query";
    private FixedQueries fixedQueries;
    private List<String> queries;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private int counter = 0;
    private String queryId;

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);

        String query = params.get(FIXED_QUERY);
        if (!query.isEmpty()) {
            queries = Arrays.asList(query.split(","));
        }

        try (FileInputStream inputStream = new FileInputStream(Paths.get(params.get(DATA_DIR), "fixedQueries.yml").toFile());) {
            fixedQueries = readFixedQueriesFromFile(Paths.get(params.get(DATA_DIR), "fixedQueries.yml"), queries);
        } catch (IOException e) {
            logger.error("Error reading file: fixedQueries.yml", e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Query generateQuery(Query query) {
        FixedQuery fixedQuery = fixedQueries.getQueries().get(counter);
        query.putAll(fixedQuery.getParams());
        appendRandomSessionId(fixedQueries.getSessionIds(), query);
        this.queryId = fixedQuery.getId();
        counter++;
        counter %= fixedQueries.getQueries().size();
        return query;
    }


    protected FixedQueries readFixedQueriesFromFile(Path path, List<String> filterQueries) throws IOException {
        FixedQueries fixedQueries = readYmlFile(path, FixedQueries.class);
        FixedQueries result = new FixedQueries();
        if (!filterQueries.isEmpty()) {
            for (FixedQuery fixedQuery : fixedQueries.getQueries()) {
                for (String query : filterQueries) {
                    if (fixedQuery.getId().equals(query)) {
                        result.addQuery(fixedQuery);
                    }
                }
            }
        }
        result.setSessionIds(fixedQueries.getSessionIds());
        return result;
    }

    @Override
    public String getQueryId() {
        return queryId;
    }
}
