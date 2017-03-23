package org.opencb.opencga.core.results;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jtarraga on 09/03/17.
 */
@Deprecated
public class VariantFacetedQueryResult<T> extends FacetedQueryResult<T> {
    public VariantFacetedQueryResult() {
        super("", -1, -1, "", "", "", new LinkedList<>());
    }

    public VariantFacetedQueryResult(String id) {
        super(id, -1, -1, "", "", "", new LinkedList<>());
    }

    public VariantFacetedQueryResult(String id, int dbTime, int numResults, String warningMsg,
                              String errorMsg, String resultType, List<FacetedQueryResultItem> result) {
        super(id, dbTime, numResults, warningMsg, errorMsg, resultType, result);
    }
}
