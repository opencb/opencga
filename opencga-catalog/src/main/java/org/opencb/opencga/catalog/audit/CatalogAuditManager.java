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

import java.util.List;

import static org.opencb.opencga.catalog.audit.AuditRecord.Resource;
import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuditManager implements AuditManager {

    protected static Logger logger = LoggerFactory.getLogger(CatalogAuditManager.class);
    private final AuditDBAdaptor auditDBAdaptor;
    private final AuthenticationManager authenticationManager;

    private static final String ROOT = "admin";

    public CatalogAuditManager(DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        this.auditDBAdaptor = catalogDBAdaptorFactory.getCatalogAuditDbAdaptor();
        this.authenticationManager = new CatalogAuthenticationManager(catalogDBAdaptorFactory, configuration.getEmail(),
                configuration.getAdmin().getSecretKey(), configuration.getAuthentication().getExpiration());
    }

    /**
     * {@inheritDoc}
     */
    @Override
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

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String token) throws CatalogException {
        if (ROOT.equals(authenticationManager.getUserId(token))) {
            return auditDBAdaptor.groupBy(query, fields, options);
        }
        throw new CatalogAuthorizationException("Only root of OpenCGA can query the audit database");
    }
}
