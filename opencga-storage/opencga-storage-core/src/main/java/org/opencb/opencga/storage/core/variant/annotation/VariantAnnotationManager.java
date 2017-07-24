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

package org.opencb.opencga.storage.core.variant.annotation;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.IOException;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantAnnotationManager {

    String SPECIES = "species";
    String ASSEMBLY = "assembly";
    String ANNOTATION_SOURCE = "annotationSource";
    String OVERWRITE_ANNOTATIONS = "overwriteAnnotations";
    String VARIANT_ANNOTATOR_CLASSNAME = "variant.annotator.classname";
    // File to load.
    String CREATE = "annotation.create";
    String LOAD_FILE = "annotation.load.file";
    String CUSTOM_ANNOTATION_KEY = "custom_annotation_key";

    void annotate(Query query, ObjectMap options) throws VariantAnnotatorException, IOException, StorageEngineException;

}
