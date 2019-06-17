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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.auth.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.AuditDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.catalog.audit.AuditRecord.Resource;
import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AuditManager {

    protected static Logger logger = LoggerFactory.getLogger(AuditManager.class);
    private final AuditDBAdaptor auditDBAdaptor;
    private final AuthenticationManager authenticationManager;

    private static final String ROOT = "admin";

    public AuditManager(DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        this.auditDBAdaptor = catalogDBAdaptorFactory.getCatalogAuditDbAdaptor();
        this.authenticationManager = new CatalogAuthenticationManager(catalogDBAdaptorFactory, configuration.getEmail(),
                configuration.getAdmin().getSecretKey(), configuration.getAuthentication().getExpiration());
    }

    /**
     * Records a login attempt.
     *
     * @param userId      User who performs the creation
     * @param success     Boolean indicating if the login was successful.
     * @throws CatalogException CatalogException
     */
    public void recordLogin(String userId, boolean success) throws CatalogException {
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
    public void recordCreation(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
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
    public void recordUpdate(Resource resource, Object id, String userId, ObjectMap update, String description, ObjectMap attributes)
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
    public void recordDeletion(Resource resource, Object id, String userId, Object object, String description, ObjectMap attributes)
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
    public void recordDeletion(Resource resource, Object id, String userId, Object before,  Object after, String description,
                                ObjectMap attributes) throws CatalogException {
        recordAction(resource, AuditRecord.Action.delete, AuditRecord.Magnitude.high, id, userId, before, after, description, attributes);
    }

    public void recordAction(Resource resource, AuditRecord.Action action, AuditRecord.Magnitude importance, Object id, String userId,
                             Object before, Object after, String description, ObjectMap attributes) throws CatalogException {
        AuditRecord auditRecord = new AuditRecord(id, resource, action, importance, toObjectMap(before), toObjectMap(after),
                System.currentTimeMillis(), userId, description, attributes);
        logger.debug("{}", action, auditRecord);
        auditDBAdaptor.insertAuditRecord(auditRecord).first();
    }

    private ObjectMap toObjectMap(Object object) {
        if (object == null) {
            return null;
        }
        ObjectMapper objectMapper = getDefaultObjectMapper();
        try {
            return new ObjectMap(objectMapper.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return new ObjectMap("object", object);
        }
    }


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
    public QueryResult groupBy(Query query, String fields, QueryOptions options, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return groupBy(query, Arrays.asList(fields.split(",")), options, sessionId);
    }

    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String token) throws CatalogException {
        if (ROOT.equals(authenticationManager.getUserId(token))) {
            return auditDBAdaptor.groupBy(query, fields, options);
        }
        throw new CatalogAuthorizationException("Only root of OpenCGA can query the audit database");
    }
}
