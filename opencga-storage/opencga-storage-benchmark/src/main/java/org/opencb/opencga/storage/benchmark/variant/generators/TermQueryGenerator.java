package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.commons.datastore.core.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by jtarraga on 07/04/17.
 */
public class TermQueryGenerator extends QueryGenerator {
    protected ArrayList<String> terms = new ArrayList<>();
    private String termFilename;
    private String queryKey;
    private Logger logger = LoggerFactory.getLogger(getClass());

    public TermQueryGenerator(String termFilename, String queryKey) {
        super();
        this.termFilename = termFilename;
        this.queryKey = queryKey;
    }

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);
        readCsvFile(Paths.get(params.get(DATA_DIR) + termFilename), strings -> terms.add(strings.get(0)));
        terms.trimToSize();
    }

    @Override
    public Query generateQuery() {
        Query query = new Query();
        query.append(queryKey, terms.get(random.nextInt(terms.size())));
        return query;
    }
}
