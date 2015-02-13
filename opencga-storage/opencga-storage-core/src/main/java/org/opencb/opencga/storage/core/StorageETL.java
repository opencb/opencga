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
public class StorageETL {

    public StorageETL() {
    }

    /**
     * This method extracts the data from the data source. This data source can be a database or a remote
     * file system. URI objects are used to allow all possibilities.
     *
     * @param from Data source origin
     * @param to   Final location of data
     */
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