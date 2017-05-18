package org.opencb.opencga.client.rest.catalog;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Family;
import org.opencb.opencga.catalog.models.acls.permissions.FamilyAclEntry;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by pfurio on 15/05/17.
 */
public class FamilyClient extends AnnotationClient<Family, FamilyAclEntry> {

    private static final String FAMILY_URL = "families";

    public FamilyClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = FAMILY_URL;
        this.clazz = Family.class;
        this.aclClass = FamilyAclEntry.class;
    }

    public QueryResponse<Family> create(String studyId, ObjectMap bodyParams) throws CatalogException, IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull(FamilyDBAdaptor.QueryParams.STUDY.key(), studyId);
        params.putIfNotNull("body", bodyParams);
        return execute(FAMILY_URL, "create", params, POST, Family.class);
    }

}
