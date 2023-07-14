package org.opencb.opencga.catalog.db.mongodb.converters;

import org.apache.avro.generic.GenericRecord;
import org.bson.Document;
import org.opencb.opencga.core.models.common.mixins.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.organizations.Organization;

public class OrganizationConverter extends OpenCgaMongoConverter<Organization> {

    public OrganizationConverter() {
        super(Organization.class);
        getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    @Override
    public Document convertToStorageType(Organization object) {
        Document document = super.convertToStorageType(object);
        document.put("uid", document.getInteger("uid").longValue());
        return document;
    }
}
