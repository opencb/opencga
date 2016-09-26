package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Annotation;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.acls.permissions.SampleAclEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public interface ISampleManager extends ResourceManager<Long, Sample>, IAnnotationSetManager {

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
    Long getId(String userId, String sampleStr) throws CatalogException;

    /**
     * Obtains the list of sampleIds corresponding to the comma separated list of sample strings given in sampleStr.
     *
     * @param userId User demanding the action.
     * @param sampleStr Comma separated list of sample ids.
     * @return A list of sample ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getIds(String userId, String sampleStr) throws CatalogException {
        List<Long> sampleIds = new ArrayList<>();
        for (String sampleId : sampleStr.split(",")) {
            sampleIds.add(getId(userId, sampleId));
        }
        return sampleIds;
    }

    @Deprecated
    Long getId(String fileId) throws CatalogException;

    QueryResult<Sample> create(long studyId, String name, String source, String description, Map<String, Object> attributes,
                               QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Annotation> load(File file) throws CatalogException;

    QueryResult<Sample> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

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
    QueryResult<SampleAclEntry> getAcls(String sampleStr, List<String> members, String sessionId) throws CatalogException;
    default List<QueryResult<SampleAclEntry>> getAcls(List<String> sampleIds, List<String> members, String sessionId)
            throws CatalogException {
        List<QueryResult<SampleAclEntry>> result = new ArrayList<>(sampleIds.size());
        for (String sampleStr : sampleIds) {
            result.add(getAcls(sampleStr, members, sessionId));
        }
        return result;
    }

    @Deprecated
    QueryResult<AnnotationSet> annotate(long sampleId, String annotationSetName, long variableSetId, Map<String, Object> annotations,
                                        Map<String, Object> attributes, boolean checkAnnotationSet,
                                        String sessionId) throws CatalogException;

    @Deprecated
    QueryResult<AnnotationSet> updateAnnotation(long sampleId, String annotationSetName, Map<String, Object> newAnnotations,
                                                String sessionId) throws CatalogException;

    @Deprecated
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
        long studyId = query.getLong(SampleDBAdaptor.QueryParams.STUDY_ID.key());
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
        long studyId = query.getLong(SampleDBAdaptor.QueryParams.STUDY_ID.key());
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
        long studyId = query.getLong(SampleDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Sample[groupBy]: Study id not found in the query");
        }
        return groupBy(studyId, query, field, options, sessionId);
    }

}
