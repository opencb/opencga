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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.IOException;
import java.net.URI;

/**
 * ETL cycle consists of the following execution steps:
 *  - extract: fetch data from different sources to be processed, eg. remote servers (S3), move to HDFS, ...
 *  - pre-transform: data is prepared to be transformed, this may include data validation and uncompression
 *  - transform: business rules are applied and some integrity checks can be applied
 *  - post-transform: some cleaning, validation or other actions can be taken into account
 *  - pre-load: transformed data can be validated or converted to physical schema in this step
 *  - load: in this step a DBWriter from getDBWriter (see below) is used to load data in the storage engine
 *  - post-load: data can be cleaned and some database validations can be performed
 *
 * Instances of this class may not be thread-safe or stateless
 *
 */
public interface StorageETL extends AutoCloseable {

    /*
     * This method extracts the data from the data source. This data source can be a database or a remote
     * file system. URI objects are used to allow all possibilities.
     *
     * @param input Data source origin
     * @param ouput Final location of data
     */
    URI extract(URI input, URI ouput) throws StorageManagerException;


    URI preTransform(URI input) throws IOException, FileFormatException, StorageManagerException;

    URI transform(URI input, URI pedigree, URI output) throws IOException, FileFormatException, StorageManagerException;

    URI postTransform(URI input) throws IOException, FileFormatException, StorageManagerException;

    default ObjectMap getTransformStats() {
        return new ObjectMap();
    }

    URI preLoad(URI input, URI output) throws IOException, StorageManagerException;

    /**
     * This method loads the transformed data file into a database, the database credentials are expected to be read
     * from configuration file.
     *
     * @param input The URI of the file to be loaded
     * @return The loaded file
     * @throws IOException If any IO problem occurs
     * @throws StorageManagerException If any other problem occurs
     */
    URI load(URI input) throws IOException, StorageManagerException;

    URI postLoad(URI input, URI output) throws IOException, StorageManagerException;

    default ObjectMap getLoadStats() {
        return new ObjectMap();
    }

    @Override
    default void close() throws StorageManagerException {}

}
