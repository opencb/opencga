package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
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
