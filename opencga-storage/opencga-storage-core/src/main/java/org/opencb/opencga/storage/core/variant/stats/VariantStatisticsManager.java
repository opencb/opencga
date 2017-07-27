/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.variant.stats;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.IOException;
import java.util.List;

/**
 * Created on 02/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantStatisticsManager {

    /**
     *
     * @param study     Study
     * @param cohorts   Cohorts to calculate stats
     * @param options   Other options
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options#AGGREGATION_MAPPING_PROPERTIES}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options#OVERWRITE_STATS}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options#UPDATE_STATS}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options#LOAD_THREADS}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options#LOAD_BATCH_SIZE}
     *                  {@link org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam#REGION}
     *
     * @throws StorageEngineException      If there is any problem related with the StorageEngine
     * @throws IOException                  If there is any IO problem
     */
    void calculateStatistics(String study, List<String> cohorts, QueryOptions options) throws IOException, StorageEngineException;

}
