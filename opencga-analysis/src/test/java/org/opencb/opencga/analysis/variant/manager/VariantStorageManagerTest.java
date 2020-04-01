/*
 * Copyright 2015-2020 OpenCB
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