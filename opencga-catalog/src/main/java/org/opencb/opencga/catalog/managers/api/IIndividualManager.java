package org.opencb.opencga.catalog.managers.api;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Individual;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public interface IIndividualManager extends ResourceManager<Integer, Individual> {

    QueryResult<Individual> create(int studyId, String name, String family, int fatherId, int motherId,
                                   Individual.Gender gender, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Individual> readAll(int studyId, QueryOptions options, String sessionId);

}
