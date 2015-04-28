package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.AnnotationSet;
import org.opencb.opencga.catalog.beans.Cohort;
import org.opencb.opencga.catalog.beans.Sample;
import org.opencb.opencga.catalog.beans.VariableSet;
import org.opencb.opencga.catalog.db.CatalogDBException;

/**
 * Created by hpccoll1 on 28/04/15.
 */
public interface CatalogSamplesDBAdaptor {

    /**
     * Samples methods
     * ***************************
     */

    public abstract boolean sampleExists(int sampleId);

    public abstract QueryResult<Sample> createSample(int studyId, Sample sample, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Sample> getSample(int sampleId, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Sample> getAllSamples(int studyId, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<Sample> modifySample(int sampleId, QueryOptions parameters) throws CatalogDBException;

    public abstract QueryResult<Integer> deleteSample(int sampleId) throws CatalogDBException;

    public abstract int getStudyIdBySampleId(int sampleId) throws CatalogDBException;

    public abstract QueryResult<Cohort> createCohort(int studyId, Cohort cohort) throws CatalogDBException;

    public abstract QueryResult<Cohort> getCohort(int cohortId) throws CatalogDBException;

    public abstract int getStudyIdByCohortId(int cohortId) throws CatalogDBException;

    /**
     * Annotation Methods
     * ***************************
     */

    public abstract QueryResult<VariableSet> createVariableSet(int studyId, VariableSet variableSet) throws CatalogDBException;

    public abstract QueryResult<VariableSet> getVariableSet(int variableSetId, QueryOptions options) throws CatalogDBException;

    public abstract QueryResult<AnnotationSet> annotateSample(int sampleId, AnnotationSet annotationSet) throws CatalogDBException;

    public abstract int getStudyIdByVariableSetId(int sampleId) throws CatalogDBException;

}
