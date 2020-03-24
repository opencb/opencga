package org.opencb.opencga.analysis.variant.manager;

import org.junit.Test;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.variant.manager.operations.AbstractVariantOperationManagerTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import static org.junit.Assert.*;

public class VariantStorageManagerTest extends AbstractVariantOperationManagerTest {

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }


    @Test
    public void testConfigure() throws CatalogException, StorageEngineException {
        ObjectMap expectedConfiguration = new ObjectMap("Key", "value");
        String existingKey = variantManager.getVariantStorageEngine(studyId, sessionId).getOptions().keySet().iterator().next();
        expectedConfiguration.put(existingKey, "OVERWRITED_VALUE");

        variantManager.configureProject(projectId, expectedConfiguration, sessionId);

        ObjectMap configuration = variantManager.getDataStoreByProjectId(projectId, sessionId).getConfiguration();
        assertEquals(expectedConfiguration, configuration);

        assertEquals("OVERWRITED_VALUE", variantManager.getVariantStorageEngine(studyId, sessionId).getOptions().get(existingKey));
    }
}