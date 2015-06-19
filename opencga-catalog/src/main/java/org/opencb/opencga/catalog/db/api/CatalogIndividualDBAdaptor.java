package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Individual;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public interface CatalogIndividualDBAdaptor {

    /**
     * Individual methods
     * ***************************
     */
    
    QueryResult<Individual> createIndividual(int studyId, Individual sample, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> getIndividual(int sampleId, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> getAllIndividuals(int studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<Individual> modifyIndividual(int sampleId, QueryOptions parameters) throws CatalogDBException;

    QueryResult<Integer> deleteIndividual(int sampleId) throws CatalogDBException;

    int getStudyIdByIndividualId(int sampleId) throws CatalogDBException;



}
