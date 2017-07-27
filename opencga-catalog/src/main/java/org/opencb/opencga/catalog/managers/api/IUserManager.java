/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.core.results.LdapImportResult;

import java.io.IOException;
import java.util.List;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public interface IUserManager extends ResourceManager<String, User> {

    /**
     * Get the userId from the sessionId.
     *
     * @param sessionId SessionId
     * @return UserId owner of the sessionId. Empty string if SessionId does not match.
     * @throws CatalogException when the session id does not correspond to any user or the token has expired.
     */
    String getId(String sessionId) throws CatalogException;

    /**
     * Create a new user.
     *
     * @param userId       User id
     * @param name         Name
     * @param email        Email
     * @param password     Encrypted Password
     * @param organization Optional organization
     * @param quota    Maximum user disk quota
     * @param type     User account type. Full or guest.
     * @param options      Optional options
     * @return The created user
     * @throws CatalogException If user already exists, or unable to create a new user.
     */
    QueryResult<User> create(String userId, String name, String email, String password, String organization, Long quota, String type,
                             QueryOptions options) throws CatalogException;

//    /**
//     * This method can only be run by the admin user. It will import users from other authentication origins such as LDAP, Kerberos, etc
//     * into catalog.
//     *
//     * @param authOrigin Id present in the catalog configuration of the authentication origin.
//     * @param accountType Type of the account to be created for the imported users (guest, full).
//     * @param params Object map containing other parameters that are useful to import users.
//     * @param adminPassword Admin password.
//     * @return A list of users that have been imported.
//     * @throws CatalogException catalogException
//     * @throws NamingException NamingException
//     */
//    List<QueryResult<User>> importFromExternalAuthOriginOld(String authOrigin, String accountType, ObjectMap params, String adminPassword)
//            throws CatalogException, NamingException;

    /**
     * This method can only be run by the admin user. It will import users and groups from other authentication origins such as LDAP,
     * Kerberos, etc into catalog.
     *
     * @param authOrigin Id present in the catalog configuration of the authentication origin.
     * @param accountType Type of the account to be created for the imported users (guest, full).
     * @param params Object map containing other parameters that are useful to import users.
     * @param adminPassword Admin password.
     * @throws CatalogException catalogException
     * @return LdapImportResult Object containing a summary of the actions performed.
     */
    LdapImportResult importFromExternalAuthOrigin(String authOrigin, String accountType, ObjectMap params, String adminPassword)
            throws CatalogException;

//    /**
//     * This method can only be run by the admin user. It will import users from groups from other authentication origins such as LDAP,
//     * Kerberos, etc into catalog.
//     *
//     * @param authOrigin Id present in the catalog configuration of the authentication origin.
//     * @param accountType Type of the account to be created for the imported users (guest, full).
//     * @param params Object map containing other parameters that are useful to import users.
//     * @param adminPassword Admin password.
//     * @throws CatalogException catalogException
//     * @throws NamingException NamingException
//     * @throws IOException IOException
//     */
//    void importGroupsFromExternalAuthOrigin(String authOrigin, String accountType, ObjectMap params, String adminPassword)
//            throws CatalogException, NamingException, IOException;

    /**
     * Delete entries from Catalog.
     *
     * @param ids       Comma separated list of ids corresponding to the objects to delete
     * @param options   Deleting options.
     * @param sessionId sessionId
     * @return A list with the deleted objects
     * @throws CatalogException CatalogException
     * @throws IOException IOException.
     */
    List<QueryResult<User>> delete(String ids, QueryOptions options, String sessionId) throws CatalogException, IOException;

    /**
     * Gets the user information.
     *
     * @param userId       User id
     * @param lastModified If lastModified matches with the one in Catalog, return an empty QueryResult.
     * @param options      QueryOptions
     * @param sessionId    SessionId of the user performing this operation.
     * @return The requested user
     * @throws CatalogException CatalogException
     */
    QueryResult<User> get(String userId, String lastModified, QueryOptions options, String sessionId) throws CatalogException;

    void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException;

    QueryResult<Session> login(String userId, String password, String sessionIp) throws CatalogException, IOException;

    QueryResult<Session> refreshToken(String userId, String token, String sessionIp) throws CatalogException, IOException;

    /**
     * This method will be only callable by the system. It generates a new session id for the user.
     *
     * @param userId user id for which a session will be generated.
     * @param adminCredentials Password or active session of the OpenCGA admin.
     * @return an objectMap containing the new sessionId
     * @throws CatalogException if the password is not correct or the userId does not exist.
     */
    QueryResult<Session> getSystemTokenForUser(String userId, String adminCredentials) throws CatalogException;

    QueryResult resetPassword(String userId, String sessionId) throws CatalogException;

    void validatePassword(String userId, String password, boolean throwException) throws CatalogException;


    /*          Filter operations     */
    /**
     * Add a new filter to the user account.
     *
     * @param userId user id to whom the filter will be associated.
     * @param sessionId session id of the user asking to store the filter.
     * @param name Filter name.
     * @param description Filter description.
     * @param bioformat Bioformat where the filter should be applied.
     * @param query Query object.
     * @param queryOptions Query options object.
     * @return the created filter.
     * @throws CatalogException if there already exists a filter with that same name for the user or if the user corresponding to the
     * session id is not the same as the provided user id.
     */
    QueryResult<User.Filter> addFilter(String userId, String sessionId, String name, String description, File.Bioformat bioformat,
                                       Query query, QueryOptions queryOptions) throws CatalogException;

    /**
     * Update the filter information.
     *
     *
     * @param userId user id to whom the filter should be updated.
     * @param sessionId session id of the user asking to update the filter.
     * @param name Filter name.
     * @param params Map containing the parameters to be updated.
     * @return the updated filter.
     * @throws CatalogException if the filter could not be updated because the filter name is not correct or if the user corresponding to
     * the session id is not the same as the provided user id.
     */
    QueryResult<User.Filter> updateFilter(String userId, String sessionId, String name, ObjectMap params) throws CatalogException;

    /**
     * Delete the filter.
     *
     *
     * @param userId user id to whom the filter should be deleted.
     * @param sessionId session id of the user asking to delete the filter.
     * @param name filter name to be deleted.
     * @return the deleted filter.
     * @throws CatalogException when the filter cannot be removed or the name is not correct or if the user corresponding to the
     * session id is not the same as the provided user id.
     */
    QueryResult<User.Filter> deleteFilter(String userId, String sessionId, String name) throws CatalogException;

    /**
     * Retrieves a filter.
     *
     * @param userId user id having the filter stored.
     * @param sessionId session id of the user fetching the filter.
     * @param name Filter name to be fetched.
     * @return the filter.
     * @throws CatalogException if the user corresponding to the session id is not the same as the provided user id.
     */
    QueryResult<User.Filter> getFilter(String userId, String sessionId, String name) throws CatalogException;

    /**
     * Retrieves all the user filters.
     *
     * @param userId user id having the filters.
     * @param sessionId session id of the user fetching the filters.
     * @return the filters.
     * @throws CatalogException if the user corresponding to the session id is not the same as the provided user id.
     */
    QueryResult<User.Filter> getAllFilters(String userId, String sessionId) throws CatalogException;

    /*        CONFIGS            */

    /**
     * Creates or updates a configuration.
     *
     * @param userId user id to whom the config will be associated.
     * @param sessionId session id of the user asking to store the config.
     * @param name Name of the configuration (normally, name of the application).
     * @param config Configuration to be stored.
     * @return the set configuration.
     * @throws CatalogException if the user corresponding to the session id is not the same as the provided user id.
     */
    QueryResult setConfig(String userId, String sessionId, String name, ObjectMap config) throws CatalogException;

    /**
     * Deletes a configuration.
     *
     * @param userId user id to whom the configuration should be deleted.
     * @param sessionId session id of the user asking to delete the configuration.
     * @param name Name of the configuration to be deleted (normally, name of the application).
     * @return the deleted configuration.
     * @throws CatalogException if the user corresponding to the session id is not the same as the provided user id or the configuration
     * did not exist.
     */
    QueryResult deleteConfig(String userId, String sessionId, String name) throws CatalogException;

    /**
     * Retrieves a configuration.
     *
     * @param userId user id having the configuration stored.
     * @param sessionId session id of the user attempting to fetch the configuration.
     * @param name Name of the configuration to be fetched (normally, name of the application).
     * @return the configuration.
     * @throws CatalogException if the user corresponding to the session id is not the same as the provided user id or the configuration
     * does not exist.
     */
    QueryResult getConfig(String userId, String sessionId, String name) throws CatalogException;

}
