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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.operations.AbstractVariantOperationManagerTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class VariantStorageManagerTest extends AbstractVariantOperationManagerTest {

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }


    @Test
    public void testConfigure() throws CatalogException, StorageEngineException {
        ObjectMap expectedConfiguration = new ObjectMap("Key", "value");
        String existingKey = variantManager.getVariantStorageEngine(studyId, sessionId).getOptions().keySet().iterator().next();
        expectedConfiguration.put(existingKey, "NEW_VALUE");

        variantManager.configureProject(projectId, expectedConfiguration, sessionId);

        ObjectMap configuration = variantManager.getDataStoreByProjectId(projectId, sessionId).getConfiguration();
        assertEquals(expectedConfiguration, configuration);

        assertEquals("NEW_VALUE", variantManager.getVariantStorageEngine(studyId, sessionId).getOptions().get(existingKey));
    }

    @Test
    public void testGetIndexedSamples() throws Exception {
        assertEquals(Collections.emptySet(), variantManager.getIndexedSamples(studyId, sessionId));
        File file = getSmallFile();
        indexFile(file, new QueryOptions(), outputId);
        assertEquals(new HashSet<>(file.getSampleIds()), variantManager.getIndexedSamples(studyId, sessionId));

        Study studyNew = catalogManager.getStudyManager().create(projectId, "sNew", "sNew", "sNew",
                "Study New", null, null, null, null, null, sessionId)
                .first();
        assertEquals(Collections.emptySet(), variantManager.getIndexedSamples(studyNew.getId(), sessionId));
    }
}