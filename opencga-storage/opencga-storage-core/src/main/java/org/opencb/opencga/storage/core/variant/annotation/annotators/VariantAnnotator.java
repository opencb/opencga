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

package org.opencb.opencga.storage.core.variant.annotation.annotators;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;

import java.io.IOException;
import java.util.List;

/**
 * Created by jacobo on 9/01/15.
 */
public abstract class VariantAnnotator {

    public VariantAnnotator(StorageConfiguration configuration, ProjectMetadata projectMetadata, ObjectMap options)
            throws VariantAnnotatorException {
    }

    /**
     * Creates variant annotations from a list of variants.
     *
     * @param variants Variants to annotate
     * @return VariantAnnotations
     * @throws VariantAnnotatorException IOException thrown
     */
    public abstract List<VariantAnnotation> annotate(List<Variant> variants) throws VariantAnnotatorException;

    public abstract ProjectMetadata.VariantAnnotatorProgram getVariantAnnotatorProgram() throws IOException;

    public abstract List<ObjectMap> getVariantAnnotatorSourceVersion() throws IOException;

}
