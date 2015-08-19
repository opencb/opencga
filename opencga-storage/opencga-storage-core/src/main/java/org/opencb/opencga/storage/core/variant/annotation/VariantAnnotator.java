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

package org.opencb.opencga.storage.core.variant.annotation;

import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

/**
 * Created by jacobo on 9/01/15.
 */
public interface VariantAnnotator {

    /**
     * Creates a variant annotation file from an specific source based on the content of a Variant DataBase.
     *
     * @param variantDBAdaptor      DBAdaptor to the variant db
     * @param outDir                File outdir.
     * @param fileName              Generated file name.
     * @param query                 Query for those variants to annotate.
     * @param options               Specific options.
     * @return                      URI of the generated file.
     * @throws IOException
     */
    URI createAnnotation(VariantDBAdaptor variantDBAdaptor, Path outDir, String fileName, Query query, QueryOptions options)
            throws IOException;

    /**
     * Loads variant annotations from an specified file into the selected Variant DataBase
     *
     * @param variantDBAdaptor      DBAdaptor to the variant db
     * @param uri                   URI of the annotation file
     * @param options               Specific options.
     * @throws IOException
     */
    void loadAnnotation(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException;

}
