package org.opencb.opencga.catalog.db.mongodb.converters;

import org.apache.avro.generic.GenericRecord;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.core.models.common.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.job.Pipeline;

public class PipelineConverter extends OpenCgaMongoConverter<Pipeline> {

    public PipelineConverter() {
        super(Pipeline.class);
        getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    @Override
    public Document convertToStorageType(Pipeline object) {
        Document document = super.convertToStorageType(object);
        document.put(MongoDBAdaptor.PRIVATE_UID, object.getUid());
        document.put(MongoDBAdaptor.PRIVATE_STUDY_UID, object.getStudyUid());
        return document;
    }

}
