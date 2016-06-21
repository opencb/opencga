package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.CohortAcl;
import org.opencb.opencga.catalog.models.acls.SampleAcl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface ISampleManager extends ResourceManager<Long, Sample> {

    Long getStudyId(long sampleId) throws CatalogException;

    /**
     * Obtains the numeric sample id given a string.
     *
     * @param userId User id of the user asking for the sample id.
     * @param sampleStr Sample id in string format. Could be one of [id | user@aliasProject:aliasStudy:sampleName
     *                | user@aliasStudy:sampleName | aliasStudy:sampleName | sampleName].
     * @return the numeric sample id.
     * @throws CatalogException when more than one sample id is found or the study or project ids cannot be resolved.
     */
    Long getSampleId(String userId, String sampleStr) throws CatalogException;

    /**
     * Obtains the list of sampleIds corresponding to the comma separated list of sample strings given in sampleStr.
     *
     * @param userId User demanding the action.
     * @param sampleStr Comma separated list of sample ids.
     * @return A list of sample ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getSampleIds(String userId, String sampleStr) throws CatalogException {
        List<Long> sampleIds = new ArrayList<>();
        for (String sampleId : sampleStr.split(",")) {
            sampleIds.add(getSampleId(userId, sampleId));
        }
        return sampleIds;
    }

    @Deprecated
    Long getSampleId(String fileId) throws CatalogException;

    /*----------------*/
    /* Sample METHODS */
    /*----------------*/

    QueryResult<Sample> create(long studyId, String name, String source, String description, Map<String, Object> attributes,
                               QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Annotation> load(File file) throws CatalogException;

    QueryResult<Sample> readAll(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Retrieve the sample Acls for the given members in the sample.
     *
     * @param sampleStr Sample id of which the acls will be obtained.
     * @param members userIds/groupIds for which the acls will be retrieved. When this is null, it will obtain all the acls.
     * @param sessionId Session of the user that wants to retrieve the acls.
     * @return A queryResult containing the sample acls.
     * @throws CatalogException when the userId does not have permissions (only the users with an "admin" role will be able to do this),
     * the sample id is not valid or the members given do not exist.
     */
    QueryResult<SampleAcl> getSampleAcls(String sampleStr, List<String> members, String sessionId) throws CatalogException;
    default List<QueryResult<SampleAcl>> getSampleAcls(List<String> sampleIds, List<String> members, String sessionId)
            throws CatalogException {
        List<QueryResult<SampleAcl>> result = new ArrayList<>(sampleIds.size());
        for (String sampleStr : sampleIds) {
            result.add(getSampleAcls(sampleStr, members, sessionId));
        }
        return result;
    }


    QueryResult<AnnotationSet> annotate(long sampleId, String annotationSetId, long variableSetId, Map<String, Object> annotations,
                                        Map<String, Object> attributes, boolean checkAnnotationSet,
                                        String sessionId) throws CatalogException;

    QueryResult<AnnotationSet> updateAnnotation(long sampleId, String annotationSetId, Map<String, Object> newAnnotations,
                                                String sessionId) throws CatalogException;

    QueryResult<AnnotationSet> deleteAnnotation(long sampleId, String annotationId, String sessionId) throws CatalogException;

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
        long studyId = query.getLong(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Sample[rank]: Study id not found in the query");
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
        long studyId = query.getLong(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Sample[groupBy]: Study id not found in the query");
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
        long studyId = query.getLong(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Sample[groupBy]: Study id not found in the query");
        }
        return groupBy(studyId, query, field, options, sessionId);
    }

    /*----------------*/
    /* Cohort METHODS */
    /*----------------*/

    long getStudyIdByCohortId(long cohortId) throws CatalogException;

    /**
     * Obtains the numeric cohort id given a string.
     *
     * @param userId User id of the user asking for the cohort id.
     * @param cohortStr Cohort id in string format. Could be one of [id | user@aliasProject:aliasStudy:cohortName
     *                | user@aliasStudy:cohortName | aliasStudy:cohortName | cohortName].
     * @return the numeric cohort id.
     * @throws CatalogException when more than one cohort id is found or .
     */
    Long getCohortId(String userId, String cohortStr) throws CatalogException;

    /**
     * Obtains the list of cohort ids corresponding to the comma separated list of cohort strings given in cohortStr.
     *
     * @param userId User demanding the action.
     * @param cohortStr Comma separated list of cohort ids.
     * @return A list of cohort ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getCohortIds(String userId, String cohortStr) throws CatalogException {
        List<Long> cohortIds = new ArrayList<>();
        for (String cohortId : cohortStr.split(",")) {
            cohortIds.add(getCohortId(userId, cohortId));
        }
        return cohortIds;
    }

    QueryResult<Cohort> readCohort(long cohortId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Cohort> readAllCohort(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Cohort> createCohort(long studyId, String name, Cohort.Type type, String description, List<Long> sampleIds,
                                     Map<String, Object> attributes, String sessionId) throws CatalogException;

    QueryResult<Cohort> updateCohort(long cohortId, ObjectMap params, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Cohort> deleteCohort(long cohortId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<AnnotationSet> annotateCohort(String cohortStr, String annotationSetId, long variableSetId, Map<String, Object> annotations,
                                        Map<String, Object> attributes, boolean checkAnnotationSet, String sessionId)
            throws CatalogException;

    QueryResult<AnnotationSet> updateCohortAnnotation(String cohortStr, String annotationSetId, Map<String, Object> newAnnotations,
                                                String sessionId) throws CatalogException;

    QueryResult<AnnotationSet> deleteCohortAnnotation(String cohortStr, String annotationId, String sessionId) throws CatalogException;

    /**
     * Retrieve the cohort Acls for the given members in the cohort.
     *
     * @param cohortStr Cohort id of which the acls will be obtained.
     * @param members userIds/groupIds for which the acls will be retrieved. When this is null, it will obtain all the acls.
     * @param sessionId Session of the user that wants to retrieve the acls.
     * @return A queryResult containing the cohort acls.
     * @throws CatalogException when the userId does not have permissions (only the users with an "admin" role will be able to do this),
     * the cohort id is not valid or the members given do not exist.
     */
    QueryResult<CohortAcl> getCohortAcls(String cohortStr, List<String> members, String sessionId) throws CatalogException;
    default List<QueryResult<CohortAcl>> getCohortAcls(List<String> cohortIds, List<String> members, String sessionId)
            throws CatalogException {
        List<QueryResult<CohortAcl>> result = new ArrayList<>(cohortIds.size());
        for (String cohortStr : cohortIds) {
            result.add(getCohortAcls(cohortStr, members, sessionId));
        }
        return result;
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
    QueryResult cohortGroupBy(long studyId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException;

    default QueryResult cohortGroupBy(Query query, List<String> field, QueryOptions options, String sessionId) throws CatalogException {
        long studyId = query.getLong(CatalogCohortDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Cohort[groupBy]: Study id not found in the query");
        }
        return cohortGroupBy(studyId, query, field, options, sessionId);
    }
}
