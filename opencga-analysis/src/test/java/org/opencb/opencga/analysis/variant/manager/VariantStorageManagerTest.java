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
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.*;

public class VariantStorageManagerTest extends AbstractVariantOperationManagerTest {

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }


    @Test
    public void testConfigure() throws CatalogException, StorageEngineException {
        ObjectMap expectedConfiguration = new ObjectMap("Key", "value").append("otherKey", "generalValue");
        String existingKey = variantManager.getVariantStorageEngine(studyId, sessionId).getOptions().keySet().iterator().next();
        expectedConfiguration.put(existingKey, "NEW_VALUE");

        ObjectMap expectedStudyConfiguration1 = new ObjectMap(expectedConfiguration)
                .append("KeyFromThisStudy", "12342134")
                .append(existingKey, "NEW_VALUE_STUDY_1");
        ObjectMap expectedStudyConfiguration2 = new ObjectMap(expectedConfiguration)
                .append("KeyFromTheSecondStudy", "afdfwef")
                .append(existingKey, "NEW_VALUE_STUDY_2");

        variantManager.configureProject(projectId, expectedConfiguration, sessionId);
        variantManager.configureStudy(studyFqn, expectedStudyConfiguration1, sessionId);
        variantManager.configureStudy(studyId2, expectedStudyConfiguration2, sessionId);

        ObjectMap configuration = variantManager.getDataStoreByProjectId(projectId, sessionId).getConfiguration();
        assertEquals(expectedConfiguration, configuration);

        VariantStorageEngine vse = variantManager.getVariantStorageEngine(studyId, sessionId);
        VariantStorageEngine vse1 = variantManager.getVariantStorageEngineForStudyOperation(studyFqn, null, sessionId);
        VariantStorageEngine vse2 = variantManager.getVariantStorageEngineForStudyOperation(studyId2, null, sessionId);

        expectedConfiguration.forEach((k, v) -> assertEquals(v, vse.getOptions().get(k)));
        expectedStudyConfiguration1.forEach((k, v) -> assertEquals(v, vse1.getOptions().get(k)));
        expectedStudyConfiguration2.forEach((k, v) -> assertEquals(v, vse2.getOptions().get(k)));
        assertEquals("NEW_VALUE", vse.getOptions().get(existingKey));
        assertEquals("NEW_VALUE_STUDY_1", vse1.getOptions().get(existingKey));

        assertNull(vse.getOptions().get("KeyFromThisStudy"));
        assertNotNull(vse1.getOptions().get("KeyFromThisStudy"));
        assertNull(vse2.getOptions().get("KeyFromThisStudy"));

        assertNull(vse.getOptions().get("KeyFromTheSecondStudy"));
        assertNull(vse1.getOptions().get("KeyFromTheSecondStudy"));
        assertNotNull(vse2.getOptions().get("KeyFromTheSecondStudy"));
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