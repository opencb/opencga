package org.opencb.opencga.catalog.models;

import org.opencb.commons.datastore.core.QueryResult;

import java.util.ArrayList;
import java.util.List;

public class InternalGetQueryResult<T> extends QueryResult<T> {

    private List<Missing> missing;

    public InternalGetQueryResult() {
    }

    public InternalGetQueryResult(String id) {
        super(id);
    }

    public InternalGetQueryResult(String id, int dbTime, int numResults, long numTotalResults, String warningMsg, String errorMsg,
                                  List<T> result) {
        super(id, dbTime, numResults, numTotalResults, warningMsg, errorMsg, result);
    }

    public InternalGetQueryResult(QueryResult<T> queryResult) {
        super(queryResult.getId(), queryResult.getDbTime(), queryResult.getNumResults(), queryResult.getNumTotalResults(),
                queryResult.getWarningMsg(), queryResult.getErrorMsg(), queryResult.getResult());
    }

    public List<Missing> getMissing() {
        return missing;
    }

    public InternalGetQueryResult<T> setMissing(List<Missing> missing) {
        this.missing = missing;
        return this;
    }

    public void addMissing(String id, String errorMsg) {
        if (this.missing == null) {
            this.missing = new ArrayList<>();
        }

        this.missing.add(new Missing(id, errorMsg));
    }

    public class Missing {
        private String id;
        private String errorMsg;

        public Missing(String id, String errorMsg) {
            this.id = id;
            this.errorMsg = errorMsg;
        }

        public String getId() {
            return id;
        }

        public String getErrorMsg() {
            return errorMsg;
        }
    }

}
