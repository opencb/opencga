package org.opencb.opencga.core.results;

import org.opencb.commons.datastore.core.QueryResult;

import java.util.List;
import java.util.Map;

/**
 * Created on 07/02/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantQueryResult<T> extends QueryResult<T> {

    private Map<String, List<String>> samples;

    public VariantQueryResult() {
        this.samples = null;
    }

    public VariantQueryResult(String id, int dbTime, int numResults, long numTotalResults, String warningMsg, String errorMsg,
                              List<T> result, Map<String, List<String>> samples) {
        super(id, dbTime, numResults, numTotalResults, warningMsg, errorMsg, result);
        this.samples = samples;
    }

    public VariantQueryResult(QueryResult<T> queryResult, Map<String, List<String>> samples) {
        super(queryResult.getId(),
                queryResult.getDbTime(),
                queryResult.getNumResults(),
                queryResult.getNumTotalResults(),
                queryResult.getWarningMsg(),
                queryResult.getErrorMsg(),
                queryResult.getResult());
        this.samples = samples;
    }

    public Map<String, List<String>> getSamples() {
        return samples;
    }

    public VariantQueryResult setSamples(Map<String, List<String>> samples) {
        this.samples = samples;
        return this;
    }
}
