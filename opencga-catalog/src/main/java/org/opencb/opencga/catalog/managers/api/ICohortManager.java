package org.opencb.opencga.catalog.managers.api;

/**
 * Created by pfurio on 06/07/16.
 */

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.Study;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Deprecated
public interface ICohortManager extends ResourceManager<Long, Cohort>, IAnnotationSetManager {

    QueryResult<Cohort> create(long studyId, String name, Study.Type type, String description, List<Long> sampleIds,
                                            Map<String, Object> attributes, String sessionId) throws CatalogException;

    Long getStudyId(long cohortId) throws CatalogException;

    /**
     * Obtains the numeric cohort id given a string.
     *
     * @param userId User id of the user asking for the cohort id.
     * @param cohortStr Cohort id in string format. Could be one of [id | user@aliasProject:aliasStudy:cohortName
     *                | user@aliasStudy:cohortName | aliasStudy:cohortName | cohortName].
     * @return the numeric cohort id.
     * @throws CatalogException when more than one cohort id is found or .
     */
    Long getId(String userId, String cohortStr) throws CatalogException;

    QueryResult<Cohort> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**     * Obtains the list of cohort ids corresponding to the comma separated list of cohort strings given in cohortStr.
     *
     * @param userId User demanding the action.
     * @param cohortStr Comma separated list of cohort ids.
     * @return A list of cohort ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getIds(String userId, String cohortStr) throws CatalogException {
        List<Long> cohortIds = new ArrayList<>();
        for (String cohortId : cohortStr.split(",")) {
            cohortIds.add(getId(userId, cohortId));
        }
        return cohortIds;
    }

    /**
     * Groups the elements queried by the field(s) given.
     *
     * @param studyId Study id.
     * @param query   Query object containing the query that will be executed.
     * @param field   Field by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the field.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(long studyId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException;

    default QueryResult groupBy(Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        long studyId = query.getLong(CohortDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Cohort[groupBy]: Study id not found in the query");
        }
        return groupBy(studyId, query, field, options, sessionId);
    }

    /**
     * Groups the elements queried by the field(s) given.
     *
     * @param studyId Study id.
     * @param query   Query object containing the query that will be executed.
     * @param fields  List of fields by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(long studyId, Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException;

    default QueryResult groupBy(Query query, List<String> field, QueryOptions options, String sessionId) throws CatalogException {
        long studyId = query.getLong(CohortDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Cohort[groupBy]: Study id not found in the query");
        }
        return groupBy(studyId, query, field, options, sessionId);
    }

    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     * @param studyId    Study id.
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param sessionId  sessionId.
     * @return           A QueryResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException;

    default QueryResult rank(Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException {
        long studyId = query.getLong(CohortDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Cohort[rank]: Study id not found in the query");
        }
        return rank(studyId, query, field, numResults, asc, sessionId);
    }
}
