package org.opencb.opencga.storage.core.search.solr;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.RETURNED_STUDIES;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDIES;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.isValidParam;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.validParams;

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
                    VariantQueryParam.UNKNOWN_GENOTYPE,
                    VariantQueryParam.INCLUDE_FORMAT,
                    VariantQueryParam.INCLUDE_GENOTYPE,
                    VariantQueryParam.SAMPLES_METADATA
                    // RETURNED_COHORTS
            )));

    private static final List<VariantField> UNSUPPORTED_VARIANT_FIELDS =
            Arrays.asList(VariantField.STUDIES_FILES,
                    VariantField.STUDIES_SAMPLES_DATA);

    public static boolean isQueryCovered(Query query) {
        for (VariantQueryParam nonCoveredParam : UNSUPPORTED_QUERY_PARAMS) {
            if (isValidParam(query, nonCoveredParam)) {
                return false;
            }
        }
        return true;
    }

    public static Collection<VariantQueryParam> coveredParams(Query query) {
        Set<VariantQueryParam> params = validParams(query);
        return coveredParams(params);
    }

    public static List<VariantQueryParam> coveredParams(Collection<VariantQueryParam> params) {
        List<VariantQueryParam> coveredParams = new ArrayList<>();

        for (VariantQueryParam param : params) {
            if (!UNSUPPORTED_QUERY_PARAMS.contains(param) && !UNSUPPORTED_MODIFIERS.contains(param)) {
                coveredParams.add(param);
            }
        }
        return coveredParams;
    }

    public static Collection<VariantQueryParam> uncoveredParams(Query query) {
        Set<VariantQueryParam> params = validParams(query);
        return uncoveredParams(params);
    }

    public static List<VariantQueryParam> uncoveredParams(Collection<VariantQueryParam> params) {
        List<VariantQueryParam> coveredParams = new ArrayList<>();

        for (VariantQueryParam param : params) {
            if (UNSUPPORTED_QUERY_PARAMS.contains(param) || UNSUPPORTED_MODIFIERS.contains(param)) {
                coveredParams.add(param);
            }
        }
        return coveredParams;
    }

    public static boolean isIncludeCovered(QueryOptions options) {
        Set<VariantField> returnedFields = VariantField.getReturnedFields(options);
        for (VariantField unsupportedVariantField : UNSUPPORTED_VARIANT_FIELDS) {
            if (returnedFields.contains(unsupportedVariantField)) {
                return false;
            }
        }
        return true;
    }

    public static Query getEngineQuery(Query query, QueryOptions options, StudyConfigurationManager scm)
            throws StorageEngineException {
        Collection<VariantQueryParam> uncoveredParams = uncoveredParams(query);
        Query engineQuery = new Query();
        for (VariantQueryParam uncoveredParam : uncoveredParams) {
            engineQuery.put(uncoveredParam.key(), query.get(uncoveredParam.key()));
        }
        // Despite STUDIES is a covered filter by Solr, it has to be in the underlying
        // query to be used as defaultStudy
        if (isValidParam(query, STUDIES)) {
            if (!uncoveredParams.isEmpty()) {
                // This will set the default study, if needed
                engineQuery.put(STUDIES.key(), query.get(STUDIES.key()));
            } else if (!isValidParam(query, RETURNED_STUDIES)) {
                // If returned studies is not defined, we need to define it with the values from STUDIES
                List<Integer> studies = VariantQueryUtils.getReturnedStudies(query, options, scm);
                engineQuery.put(RETURNED_STUDIES.key(), studies);
            }
        }
        return engineQuery;
    }
}
