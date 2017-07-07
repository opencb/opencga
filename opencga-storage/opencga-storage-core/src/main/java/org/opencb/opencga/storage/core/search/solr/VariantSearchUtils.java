package org.opencb.opencga.storage.core.search.solr;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.isValidParam;

/**
 * Created on 06/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSearchUtils {

    public static final Set<VariantQueryParam> UNSUPPORTED_QUERY_PARAMS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList(VariantQueryParam.FILES,
                    VariantQueryParam.FILTER,
                    VariantQueryParam.GENOTYPE,
                    VariantQueryParam.SAMPLES)));

    public static final Set<VariantQueryParam> UNSUPPORTED_MODIFIERS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList(VariantQueryParam.RETURNED_FILES,
                    VariantQueryParam.RETURNED_SAMPLES,
                    VariantQueryParam.RETURNED_STUDIES,
                    VariantQueryParam.UNKNOWN_GENOTYPE
                    // RETURNED_COHORTS
                    // INCLUDED_FORMATS
            )));

    public static boolean isQueryCovered(Query query) {
        for (VariantQueryParam nonCoveredParam : UNSUPPORTED_QUERY_PARAMS) {
            if (isValidParam(query, nonCoveredParam)) {
                return false;
            }
        }
        return true;
    }

    public static List<VariantQueryParam> coveredParams(Collection<VariantQueryParam> params) {
        List<VariantQueryParam> coveredParams = new ArrayList<>();

        for (VariantQueryParam param : params) {
            if (!UNSUPPORTED_QUERY_PARAMS.contains(param)) {
                coveredParams.add(param);
            }
        }
        return coveredParams;
    }

    public static List<VariantQueryParam> uncoveredParams(Collection<VariantQueryParam> params) {
        List<VariantQueryParam> coveredParams = new ArrayList<>();

        for (VariantQueryParam param : params) {
            if (UNSUPPORTED_QUERY_PARAMS.contains(param)) {
                coveredParams.add(param);
            }
        }
        return coveredParams;
    }

    public static boolean isIncludeCovered(QueryOptions options) {
        Set<VariantField> returnedFields = VariantField.getReturnedFields(options);
        return options.getBoolean(VariantSearchManager.SUMMARY)
                || (!returnedFields.contains(VariantField.STUDIES_FILES) && !returnedFields.contains(VariantField.STUDIES_SAMPLES_DATA));
    }

}
