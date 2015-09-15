package org.opencb.opencga.catalog.managers.api;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
* @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
*/
public interface IStudyManager extends ResourceManager<Integer, Study> {

    String  getUserId(int studyId) throws CatalogException;

    Integer getProjectId(int studyId) throws CatalogException;

    Integer getStudyId(String studyId) throws CatalogException;

    /**
     * Creates a new Study in catalog
     * @param projectId     Parent project id
     * @param name          Study Name
     * @param alias         Study Alias. Must be unique in the project's studies
     * @param type          Study type: CONTROL_CASE, CONTROL_SET, ... (see org.opencb.opencga.catalog.models.Study.Type)
     * @param creatorId     Creator user id. If null, user by sessionId
     * @param creationDate  Creation date. If null, now
     * @param description   Study description. If null, empty string
     * @param status        Unused
     * @param cipher        Unused
     * @param uriScheme     UriScheme to select the CatalogIOManager. Default: CatalogIOManagerFactory.DEFAULT_CATALOG_SCHEME
     * @param uri           URI for the folder where to place the study. Scheme must match with the uriScheme. Folder must exist.
     * @param datastores    DataStores information
     * @param stats         Optional stats
     * @param attributes    Optional attributes
     * @param options       QueryOptions
     * @param sessionId     User's sessionId
     * @return              Generated study
     * @throws CatalogException
     */
    QueryResult<Study> create(int projectId, String name, String alias, Study.Type type,
                                     String creatorId, String creationDate, String description, String status,
                                     String cipher, String uriScheme, URI uri, Map<File.Bioformat, DataStore> datastores, Map<String, Object> stats,
                                     Map<String, Object> attributes, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Study> share(int studyId, AclEntry acl) throws CatalogException;


    /*---------------------*/
    /* VariableSet METHODS */
    /*---------------------*/

    QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique, String description,
                                               Map<String, Object> attributes, List<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique, String description,
                                               Map<String, Object> attributes, Set<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> readVariableSet(int variableSet, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<VariableSet> readAllVariableSets(int studyId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<VariableSet> deleteVariableSet(int variableSetId, QueryOptions queryOptions, String sessionId) throws CatalogException;

}
