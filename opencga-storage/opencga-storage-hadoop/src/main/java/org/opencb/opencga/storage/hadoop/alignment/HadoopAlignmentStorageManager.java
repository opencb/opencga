/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.hadoop.alignment;

import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;

import java.io.IOException;
import java.net.URI;

/**
 * Created by imedina on 16/06/15.
 */
public class HadoopAlignmentStorageManager extends AlignmentStorageManager {

    @Override
    public URI preLoad(URI input, URI output) throws IOException, StorageManagerException {
        return null;
    }

    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        return null;
    }

    @Override
    public URI postLoad(URI input, URI output) throws IOException, StorageManagerException {
        return null;
    }

    @Override
    public DataWriter<AlignmentRegion> getDBWriter(String dbName) throws StorageManagerException {
        return null;
    }

    @Override
    public AlignmentDBAdaptor getDBAdaptor(String dbName) throws StorageManagerException {
        return null;
    }

}
