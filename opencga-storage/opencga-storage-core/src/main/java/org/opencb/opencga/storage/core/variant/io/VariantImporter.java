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

package org.opencb.opencga.storage.core.variant.io;

import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ExportMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created on 07/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantImporter {

    protected final VariantDBAdaptor dbAdaptor;

    public VariantImporter(VariantDBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
    }

    public void importData(URI inputUri) throws StorageEngineException, IOException {
        VariantMetadataImporter metadataImporter = new VariantMetadataImporter();
        ExportMetadata exportMetadata = metadataImporter.importMetaData(inputUri, dbAdaptor.getStudyConfigurationManager());

        importData(inputUri, exportMetadata);
    }

    public void importData(URI input, ExportMetadata remappedMetadata) throws StorageEngineException, IOException {
        Map<StudyConfiguration, StudyConfiguration> map = remappedMetadata.getStudies().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        importData(input, remappedMetadata, map);
    }

    public abstract void importData(URI input, ExportMetadata remappedMetadata,
                                    Map<StudyConfiguration, StudyConfiguration> studiesOldNewMap)
            throws StorageEngineException, IOException;

}
