package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.VariableSet;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogSampleDBAdaptor {

    /**
     * Samples methods
     * ***************************
     */

    boolean sampleExists(int sampleId);

    QueryResult<Sample> createSample(int studyId, Sample sample, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> getSample(int sampleId, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> getAllSamples(int studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<Sample> modifySample(int sampleId, QueryOptions parameters) throws CatalogDBException;

    QueryResult<Integer> deleteSample(int sampleId) throws CatalogDBException;

    int getStudyIdBySampleId(int sampleId) throws CatalogDBException;

    QueryResult<Cohort> createCohort(int studyId, Cohort cohort) throws CatalogDBException;

    QueryResult<Cohort> getCohort(int cohortId) throws CatalogDBException;

    int getStudyIdByCohortId(int cohortId) throws CatalogDBException;

    /**
     * Annotation Methods
     * ***************************
     */

    QueryResult<VariableSet> createVariableSet(int studyId, VariableSet variableSet) throws CatalogDBException;

    QueryResult<VariableSet> getVariableSet(int variableSetId, QueryOptions options) throws CatalogDBException;

    QueryResult<AnnotationSet> annotateSample(int sampleId, AnnotationSet annotationSet) throws CatalogDBException;

    int getStudyIdByVariableSetId(int sampleId) throws CatalogDBException;

}
