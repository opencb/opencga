package org.opencb.opencga.core.results;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jtarraga on 09/03/17.
 */
public class FacetedQueryResult<T> {
    private String id;
    private int dbTime;
    private int numResults;
    private String warningMsg;
    private String errorMsg;
    private String resultType;
    private List<FacetedQueryResultItem> result;
//    private Class<T> clazz;

    public FacetedQueryResult() {
        this("", -1, -1, "", "", "", new LinkedList<>());
    }

    public FacetedQueryResult(String id) {
        this(id, -1, -1, "", "", "", new LinkedList<>());
    }

    public FacetedQueryResult(String id, int dbTime, int numResults, String warningMsg,
                              String errorMsg, String resultType, List<FacetedQueryResultItem> result) {
        this.id = id;
        this.dbTime = dbTime;
        this.numResults = numResults;
        this.warningMsg = warningMsg;
        this.errorMsg = errorMsg;
        this.resultType = resultType;
        this.result = result;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getDbTime() {
        return dbTime;
    }

    public void setDbTime(int dbTime) {
        this.dbTime = dbTime;
    }

    public int getNumResults() {
        return numResults;
    }

    public void setNumResults(int numResults) {
        this.numResults = numResults;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getResultType() {
        return resultType;
    }

    public void setResultType(String resultType) {
        this.resultType = resultType;
    }

    public List<FacetedQueryResultItem> getResult() {
        return result;
    }

    public void setResult(List<FacetedQueryResultItem> result) {
        this.result = result;
    }
}
