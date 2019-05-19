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

package org.opencb.opencga.storage.core.alignment.local;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

/**
 * Created by pfurio on 07/11/16.
 */
public class LocalAlignmentStorageEngine extends AlignmentStorageEngine {

    private AlignmentDBAdaptor dbAdaptor;

    public LocalAlignmentStorageEngine() {
        super();
        this.dbAdaptor = new LocalAlignmentDBAdaptor();
    }

    @Override
    public AlignmentDBAdaptor getDBAdaptor() throws StorageEngineException {
        return dbAdaptor;
    }

    @Override
    public void testConnection() throws StorageEngineException {
    }

    @Override
    public StoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException {
        StorageEtlConfiguration etlConfiguration;
        if (getConfiguration() == null
                || getConfiguration().getStorageEngine() == null
                || getConfiguration().getStorageEngine().getAlignment() == null) {
            etlConfiguration = new StorageEtlConfiguration();
        } else {
            etlConfiguration = getConfiguration().getStorageEngine().getAlignment();
        }
        if (etlConfiguration.getOptions() == null) {
            etlConfiguration.setOptions(new ObjectMap());
        }
        return new LocalAlignmentStoragePipeline(etlConfiguration);
    }

}
