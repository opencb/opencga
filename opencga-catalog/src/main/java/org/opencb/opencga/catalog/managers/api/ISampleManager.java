package org.opencb.opencga.catalog.managers.api;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
* @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
*/
public interface ISampleManager extends ResourceManager<Integer, Sample> {
    Integer getStudyId(int sampleId) throws CatalogException;

    QueryResult<AnnotationSet> annotate(int sampleId, String id, int variableSetId, Map<String, Object> annotations,
                                        Map<String, Object> attributes, boolean checkAnnotationSet, String sessionId)
            throws CatalogException;

    QueryResult<Annotation> load(File file) throws CatalogException;

    QueryResult<Sample> create(int studyId, String name, String source, String description,
                               Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<Sample> readAll(int studyId, QueryOptions query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique,
                                               String description, Map<String, Object> attributes,
                                               List<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique,
                                               String description, Map<String, Object> attributes,
                                               Set<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> readVariableset(int variableSet, QueryOptions options, String sessionId) throws CatalogException;

    int getStudyIdByCohortId(int cohortId) throws CatalogException;

    QueryResult<Cohort> readCohort(int cohortId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Cohort> createCohort(int studyId, String name, String description, List<Integer> sampleIds,
                                     Map<String, Object> attributes, String sessionId) throws CatalogException;
}
