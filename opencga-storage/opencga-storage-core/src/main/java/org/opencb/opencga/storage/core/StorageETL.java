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

package org.opencb.opencga.storage.core;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.datastore.core.ObjectMap;

import java.io.IOException;
import java.net.URI;

/**
 * ETL cycle consists of the following execution steps:
 * extract: fetch data from different sources to be processed, eg. remote servers (S3), move to HDFS, ...
 * pre-transform: data is prepared to be transformed, this may include data validation and uncompression
 * transform: business rules are applied and some integrity checks can be applied
 * post-transform: some cleaning, validation or other actions can be taken into account
 * pre-load: transformed data can be validated or converted to physical schema in this step
 * load: in this step a DBWriter from getDBWriter (see below) is used to load data in the storage engine
 * post-load: data can be cleaned and some database validations can be performed
 */
@Deprecated
public class StorageETL {

    public StorageETL() {
    }

    public final URI extract(URI from, URI to, ObjectMap params) {
        return null;
    }

    public final URI preTransform(URI input, ObjectMap params) throws IOException, FileFormatException {
        return null;
    }

    public final URI transform(URI input, URI pedigree, URI output, ObjectMap params) throws IOException, FileFormatException {
        return null;
    }

    public final URI postTransform(URI input, ObjectMap params) throws IOException, FileFormatException {
        return null;
    }

    public final URI preLoad(URI input, URI output, ObjectMap params) throws IOException {
        return null;
    }

    public final URI load(URI input, ObjectMap params) throws IOException {
        return null;
    }

    public final URI postLoad(URI input, URI output, ObjectMap params) throws IOException {
        return null;
    }

}
