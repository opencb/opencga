package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQueries;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQuery;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
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
    private Path outDir;

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);

        Path queryFilePath;
        String queryFile = params.get(FILE);
        outDir = Paths.get(params.get(OUT_DIR));
        queryId = params.get(FIXED_QUERY);

        if (queryFile == null || queryFile.isEmpty()) {
            queryFile = params.get(DATA_DIR).concat("/").concat(FIXED_QUERIES_FILE);
        }

        queryFilePath = Paths.get(queryFile);
        fixedQueries = readYmlFile(queryFilePath, FixedQueries.class);
        createUserPropertiesFile();

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

    private void createUserPropertiesFile() {
        Path userPropertyFile = Paths.get(outDir.toString(), USER_PROPERTIES_FILE);

        if (!Files.exists(userPropertyFile)) {
            try (PrintWriter printWriter = new PrintWriter(new FileWriter(userPropertyFile.toFile()))) {
                StringBuilder st = new StringBuilder();
                for (FixedQuery fixedQuery : fixedQueries.getQueries()) {
                    int th = fixedQuery.getTolerationThreshold();
                    st.append(fixedQuery.getId() + ":" + th + "|" + (int) (th + (th * 15.0 / 100)) + ";" + "\\\n");
                }
                printWriter.println("jmeter.reportgenerator.apdex_per_transaction=" + st);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
