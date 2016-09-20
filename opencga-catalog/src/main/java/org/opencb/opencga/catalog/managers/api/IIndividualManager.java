package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.acls.permissions.IndividualAclEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hpccoll1 on 19/06/15.
 */
@Deprecated
public interface IIndividualManager extends ResourceManager<Long, Individual>, IAnnotationSetManager {

    Long getStudyId(long individualId) throws CatalogException;

    /**
     * Obtains the numeric individual id given a string.
     *
     * @param userId User id of the user asking for the individual id.
     * @param individualStr Individual id in string format. Could be one of [id | user@aliasProject:aliasStudy:individualName
     *                | user@aliasStudy:individualName | aliasStudy:individualName | individualName].
     * @return the numeric individual id.
     * @throws CatalogException when more than one individual id is found or the study or project ids cannot be resolved.
     */
    Long getId(String userId, String individualStr) throws CatalogException;

    /**
     * Obtains the list of individualIds corresponding to the comma separated list of individual strings given in individualStr.
     *
     * @param userId User demanding the action.
     * @param individualStr Comma separated list of individual ids.
     * @return A list of individual ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getIds(String userId, String individualStr) throws CatalogException {
        List<Long> individualIds = new ArrayList<>();
        for (String individualId : individualStr.split(",")) {
            individualIds.add(getId(userId, individualId));
        }
        return individualIds;
    }

    @Deprecated
    Long getId(String individualId) throws CatalogException;

    QueryResult<Individual> create(long studyId, String name, String family, long fatherId, long motherId, Individual.Sex sex,
                                   QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Individual> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Retrieve the individual Acls for the given members in the individual.
     *
     * @param individualStr Individual id of which the acls will be obtained.
     * @param members userIds/groupIds for which the acls will be retrieved. When this is null, it will obtain all the acls.
     * @param sessionId Session of the user that wants to retrieve the acls.
     * @return A queryResult containing the individual acls.
     * @throws CatalogException when the userId does not have permissions (only the users with an "admin" role will be able to do this),
     * the individual id is not valid or the members given do not exist.
     */
    @Deprecated
    QueryResult<IndividualAclEntry> getAcls(String individualStr, List<String> members, String sessionId) throws CatalogException;
    @Deprecated
    default List<QueryResult<IndividualAclEntry>> getAcls(List<String> individualIds, List<String> members, String sessionId)
            throws CatalogException {
        List<QueryResult<IndividualAclEntry>> result = new ArrayList<>(individualIds.size());
        for (String individualStr : individualIds) {
            result.add(getAcls(individualStr, members, sessionId));
        }
        return result;
    }

    @Deprecated
    QueryResult<AnnotationSet> annotate(long individualId, String annotationSetName, long variableSetId, Map<String, Object> annotations,
                                        Map<String, Object> attributes, String sessionId) throws CatalogException;

    @Deprecated
    QueryResult<AnnotationSet> updateAnnotation(long individualId, String annotationSetName, Map<String, Object> newAnnotations,
                                                String sessionId) throws CatalogException;

    @Deprecated
    QueryResult<AnnotationSet> deleteAnnotation(long individualId, String annotationId, String sessionId) throws CatalogException;

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
        long studyId = query.getLong(IndividualDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Individual[rank]: Study id not found in the query");
        }
        return rank(studyId, query, field, numResults, asc, sessionId);
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
        long studyId = query.getLong(IndividualDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Individual[groupBy]: Study id not found in the query");
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
        long studyId = query.getLong(IndividualDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Individual[groupBy]: Study id not found in the query");
        }
        return groupBy(studyId, query, field, options, sessionId);
    }

}
