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

package org.opencb.opencga.storage.core.variant;

import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantType;

/**
 * Created on 26/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantStorageTest extends AutoCloseable {

    VariantStorageEngine getVariantStorageEngine() throws Exception;

    void clearDB(String dbName) throws Exception;

    default void close() throws Exception {}

    default int getExpectedNumLoadedVariants(VariantFileMetadata fileMetadata) {
        int numRecords = fileMetadata.getStats().getVariantCount();
        return numRecords
                - fileMetadata.getStats().getTypeCount().getOrDefault(VariantType.SYMBOLIC.name(), 0)
                - fileMetadata.getStats().getTypeCount().getOrDefault(VariantType.NO_VARIATION.name(), 0);
    }
}
