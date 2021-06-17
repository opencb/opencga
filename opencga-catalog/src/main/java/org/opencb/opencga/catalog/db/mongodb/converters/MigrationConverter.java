package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.opencga.catalog.migration.MigrationRun;

public class MigrationConverter extends OpenCgaMongoConverter<MigrationRun> {

    public MigrationConverter() {
        super(MigrationRun.class);
    }

    @Override
    public MigrationRun convertToDataModelType(Document document) {
        return super.convertToDataModelType(document);
    }

    @Override
    public Document convertToStorageType(MigrationRun object) {
        return super.convertToStorageType(object);
    }


}
