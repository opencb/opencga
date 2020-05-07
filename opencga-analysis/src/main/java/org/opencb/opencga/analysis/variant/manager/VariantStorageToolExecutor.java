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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.ConfigurationUtils;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.IOException;

/**
 * Helper interface to be used by opencga local analysis executors.
 */
@Deprecated
// Please, use the StorageToolExecutor
public interface VariantStorageToolExecutor {

    ObjectMap getExecutorParams();

    default VariantStorageManager getVariantStorageManager() throws ToolExecutorException {
        String opencgaHome = getExecutorParams().getString("opencgaHome");
        if (StringUtils.isEmpty(opencgaHome)) {
            throw new ToolExecutorException("Missing arguments!");
        }
        try {
            Configuration configuration = ConfigurationUtils.loadConfiguration(opencgaHome);
            CatalogManager catalogManager = new CatalogManager(configuration);
            StorageEngineFactory engineFactory = StorageEngineFactory.get();
            return new VariantStorageManager(catalogManager, engineFactory);
        } catch (CatalogException | IOException e) {
            throw new ToolExecutorException(e);
        }
    }

}
