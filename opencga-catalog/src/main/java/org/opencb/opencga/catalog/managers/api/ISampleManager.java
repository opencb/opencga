package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;

import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface ISampleManager extends ResourceManager<Long, Sample> {

    Long getStudyId(long sampleId) throws CatalogException;

    /*----------------*/
    /* Sample METHODS */
    /*----------------*/

    QueryResult<Sample> create(long studyId, String name, String source, String description, Map<String, Object> attributes,
                               QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Annotation> load(File file) throws CatalogException;

    QueryResult<Sample> readAll(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

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

    QueryResult<Cohort> readCohort(long cohortId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Cohort> readAllCohort(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Cohort> createCohort(long studyId, String name, Cohort.Type type, String description, List<Long> sampleIds,
                                     Map<String, Object> attributes, String sessionId) throws CatalogException;

    QueryResult<Cohort> updateCohort(long cohortId, ObjectMap params, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Cohort> deleteCohort(long cohortId, QueryOptions options, String sessionId) throws CatalogException;


}
