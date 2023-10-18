package org.opencb.opencga.catalog.db.mongodb.converters;

import org.apache.avro.generic.GenericRecord;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBUtils;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.common.mixins.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.organizations.Organization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OrganizationConverter extends OpenCgaMongoConverter<Organization> {

    public OrganizationConverter() {
        super(Organization.class);
        getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    public List<Document> convertAuthenticationOrigins(List<AuthenticationOrigin> authenticationOriginList) throws CatalogDBException {
        if (authenticationOriginList == null || authenticationOriginList.isEmpty()) {
            return Collections.emptyList();
        }
        List<Document> authenticationOriginDocumentList = new ArrayList<>(authenticationOriginList.size());
        for (AuthenticationOrigin authenticationOrigin : authenticationOriginList) {
            Document authOriginDocument = MongoDBUtils.getMongoDBDocument(authenticationOrigin, "AuthenticationOrigin");
            authenticationOriginDocumentList.add(authOriginDocument);
        }
        return authenticationOriginDocumentList;

    }
}
