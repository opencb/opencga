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

package org.opencb.opencga.catalog.audit;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditRecord.Resource;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.util.Arrays;
import java.util.List;

/**
 * Created on 18/08/15.
 * <p>
 * Create the AuditRecord from simple params
 * Select which actions will be recorded
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface AuditManager {

    /**
     * Records a login attempt.
     *
     * @param userId      User who performs the creation
     * @param success     Boolean indicating if the login was successful.
     * @throws CatalogException CatalogException
     */
    default void recordLogin(String userId, boolean success) throws CatalogException {
        if (success) {
            recordAction(Resource.user, AuditRecord.Action.login, AuditRecord.Magnitude.low, userId, userId, null, null,
                    "User successfully logged in", null);
        } else {
            recordAction(Resource.user, AuditRecord.Action.login, AuditRecord.Magnitude.high, userId, userId, null, null,
                    "Wrong user or password", null);
        }
    }


    /**
     * Records an object creation over the Catalog Database.
     *
     * @param resource    Resource type
     * @param id          Resource id (either String or Integer)
     * @param userId      User who performs the creation
     * @param object      Created object
     * @param description Optional description
     * @param attributes  Optional attributes
     * @throws CatalogException CatalogException
     */
    default void recordCreation(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
            throws CatalogException {
        recordAction(resource, AuditRecord.Action.create, AuditRecord.Magnitude.low, id, userId, null, object, description, attributes);
    }

    /**
     * Record an atomic change over the Catalog Database.
     *
     * @param resource    Resource type
     * @param id          Resource id (either String or Integer)
     * @param userId      User who performs the update
     * @param update      Update params
     * @param description Optional description
     * @param attributes  Optional attributes
     * @throws CatalogException CatalogException
     */
    default void recordUpdate(Resource resource, Object id, String userId, ObjectMap update, String description, ObjectMap attributes)
            throws CatalogException {
        recordAction(resource, AuditRecord.Action.update, AuditRecord.Magnitude.medium, id, userId, null, update, description, attributes);
    }

    /**
     * Records a permanent deletion over the Catalog Database.
     *
     * @param resource    Resource type
     * @param id          Resource id (either String or Integer)
     * @param userId      User who performs the deletion
     * @param object      Deleted object
     * @param description Optional description
     * @param attributes  Optional attributes
     * @throws CatalogException CatalogException
     */
    default void recordDeletion(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
            throws CatalogException {
        recordAction(resource, AuditRecord.Action.delete, AuditRecord.Magnitude.high, id, userId, object, null, description, attributes);
    }

    /**
     * Records a deletion (change of state) over the Catalog Database.
     *
     * @param resource    Resource type
     * @param id          Resource id (either String or Integer)
     * @param userId      User who performs the deletion
     * @param before      Previous object state
     * @param after       Posterior object state
     * @param description Optional description
     * @param attributes  Optional attributes
     * @throws CatalogException CatalogException
     */
    default void recordDeletion(Resource resource, Object id, String userId, Object before,  Object after, String description,
                                ObjectMap attributes) throws CatalogException {
        recordAction(resource, AuditRecord.Action.delete, AuditRecord.Magnitude.high, id, userId, before, after, description, attributes);
    }

    /**
     * Records a restore over the Catalog Database.
     *
     * @param resource    Resource type
     * @param id          Resource id (either String or Integer)
     * @param userId      User who performs the deletion
     * @param before      Previous object state
     * @param after       Posterior object state
     * @param description Optional description
     * @param attributes  Optional attributes
     * @throws CatalogException CatalogException
     */
    default void recordRestore(Resource resource, Object id, String userId, Object before,  Object after, String description,
                                ObjectMap attributes) throws CatalogException {
        if (before instanceof String && StringUtils.isNotEmpty((String) before)) {
            before = new ObjectMap("status", before);
        }
        if (after instanceof String && StringUtils.isNotEmpty((String) after)) {
            after = new ObjectMap("status", after);
        }
        recordAction(resource, AuditRecord.Action.restore, AuditRecord.Magnitude.high, id, userId, before, after, description, attributes);
    }

    /**
     * Records an object creation over the Catalog Database.
     *
     * @param resource    Resource type
     * @param action      Executed action
     * @param importance  Importance of the document being audited (high, medium or low)
     * @param id          Resource id (either String or Integer)
     * @param userId      User who performs the action
     * @param before      Optional Previous object state
     * @param after       Optional Posterior object state
     * @param description Optional description
     * @param attributes  Optional attributes
     * @throws CatalogException CatalogException
     */
    void recordAction(Resource resource, AuditRecord.Action action, AuditRecord.Magnitude importance, Object id, String userId,
                             Object before, Object after, String description, ObjectMap attributes)
            throws CatalogException;

    /**
     * Groups the matching entries by some fields.
     *
     * @param query     Query object.
     * @param fields    A field or a comma separated list of fields by which the results will be grouped in.
     * @param options   QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    default QueryResult groupBy(Query query, String fields, QueryOptions options, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return groupBy(query, Arrays.asList(fields.split(",")), options, sessionId);
    }

    /**
     * Groups the matching entries by some fields.
     *
     * @param query     Query object.
     * @param options   QueryOptions object.
     * @param fields    A field or a comma separated list of fields by which the results will be grouped in.
     * @param sessionId Session id of the user logged in.
     * @return A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException;

}
