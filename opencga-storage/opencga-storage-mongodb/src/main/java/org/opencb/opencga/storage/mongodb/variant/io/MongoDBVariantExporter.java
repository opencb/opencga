package org.opencb.opencga.storage.mongodb.variant.io;

import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;

/**
 * MongoDB variant exporter.
 */
public class MongoDBVariantExporter extends VariantExporter {

    public MongoDBVariantExporter(VariantStorageEngine engine, VariantMetadataFactory metadataFactory,
                                  IOConnectorProvider ioConnectorProvider) throws StorageEngineException {
        super(engine, metadataFactory, ioConnectorProvider);
    }
}
