package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface IStudyManager extends ResourceManager<Long, Study> {

    String getUserId(long studyId) throws CatalogException;

    Long getProjectId(long studyId) throws CatalogException;

    Long getStudyId(String studyId) throws CatalogException;

    /**
     * Creates a new Study in catalog.
     *
     * @param projectId    Parent project id
     * @param name         Study Name
     * @param alias        Study Alias. Must be unique in the project's studies
     * @param type         Study type: CONTROL_CASE, CONTROL_SET, ... (see org.opencb.opencga.catalog.models.Study.Type)
     * @param creatorId    Creator user id. If null, user by sessionId
     * @param creationDate Creation date. If null, now
     * @param description  Study description. If null, empty string
     * @param status       Unused
     * @param cipher       Unused
     * @param uriScheme    UriScheme to select the CatalogIOManager. Default: CatalogIOManagerFactory.DEFAULT_CATALOG_SCHEME
     * @param uri          URI for the folder where to place the study. Scheme must match with the uriScheme. Folder must exist.
     * @param datastores   DataStores information
     * @param stats        Optional stats
     * @param attributes   Optional attributes
     * @param options      QueryOptions
     * @param sessionId    User's sessionId
     * @return Generated study
     * @throws CatalogException CatalogException
     */
    QueryResult<Study> create(long projectId, String name, String alias, Study.Type type, String creatorId, String creationDate,
                              String description, Status status, String cipher, String uriScheme, URI uri,
                              Map<File.Bioformat, DataStore> datastores, Map<String, Object> stats, Map<String, Object> attributes,
                              QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Study> share(long studyId, AclEntry acl) throws CatalogException;


    /*---------------------*/
    /* VariableSet METHODS */
    /*---------------------*/

    QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique, String description,
                                               Map<String, Object> attributes, List<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique, String description,
                                               Map<String, Object> attributes, Set<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> readVariableSet(long variableSet, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<VariableSet> readAllVariableSets(long studyId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<VariableSet> deleteVariableSet(long variableSetId, QueryOptions queryOptions, String sessionId) throws CatalogException;

    QueryResult<VariableSet> addFieldToVariableSet(long variableSetId, Variable variable, String sessionId) throws CatalogException;

    QueryResult<VariableSet> removeFieldFromVariableSet(long variableSetId, String fieldId, String sessionId) throws CatalogException;

    QueryResult<VariableSet> renameFieldFromVariableSet(long variableSetId, String oldName, String newName, String sessionId)
            throws CatalogException;

}
