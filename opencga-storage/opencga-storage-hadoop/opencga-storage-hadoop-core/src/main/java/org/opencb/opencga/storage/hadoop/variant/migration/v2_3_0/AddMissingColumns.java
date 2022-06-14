package org.opencb.opencga.storage.hadoop.variant.migration.v2_3_0;

import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchemaManager;

public class AddMissingColumns implements Runnable {

    private final HadoopVariantStorageEngine engine;

    public AddMissingColumns(Object engine) {
        this.engine = (HadoopVariantStorageEngine) engine;
    }

    @Override
    public void run() {
        try {
            new VariantPhoenixSchemaManager(this.engine.getDBAdaptor()).registerAnnotationColumns();
        } catch (StorageEngineException e) {
            throw new RuntimeException(e);
        }
    }

}
