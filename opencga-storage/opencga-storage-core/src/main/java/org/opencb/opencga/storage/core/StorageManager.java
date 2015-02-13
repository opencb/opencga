package org.opencb.opencga.storage.core;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.datastore.core.ObjectMap;

import java.io.IOException;
import java.net.URI;

/**
 * @author imedina
 * @param <DBWRITER>
 * @param <DBADAPTOR>
 */
public interface StorageManager<DBWRITER, DBADAPTOR> {

    public void addConfigUri(URI configUri);

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


    /**
     * This method extracts the data from the data source. This data source can be a database or a remote
     * file system. URI objects are used to allow all possibilities.
     * @param from Data source origin
     * @param to Final location of data
     */
    public URI extract(URI from, URI to, ObjectMap params) throws StorageManagerException;


    public URI preTransform(URI input, ObjectMap params) throws IOException, FileFormatException, StorageManagerException;

    public URI transform(URI input, URI pedigree, URI output, ObjectMap params) throws IOException, FileFormatException, StorageManagerException;

    public URI postTransform(URI input, ObjectMap params) throws IOException, FileFormatException, StorageManagerException;


    public URI preLoad(URI input, URI output, ObjectMap params) throws IOException, StorageManagerException;

    public URI load(URI input, ObjectMap params) throws IOException, StorageManagerException;

    public URI postLoad(URI input, URI output, ObjectMap params) throws IOException, StorageManagerException;
//
//    public final URI preTransform(URI input, ObjectMap params) throws IOException, FileFormatException {
//        return getETL().preTransform(input, params);
//    }
//
//    public final URI transform(URI input, URI pedigree, URI output, ObjectMap params) throws IOException, FileFormatException {
//        return getETL().transform(input, pedigree, output, params);
//    }
//
//    public final URI postTransform(URI input, ObjectMap params) throws IOException, FileFormatException {
//        return getETL().postTransform(input, params);
//    }
//
//
//    public final URI preLoad(URI input, URI output, ObjectMap params) throws IOException {
//        return getETL().preLoad(input, output, params);
//    }
//
//    public final URI load(URI input, ObjectMap params) throws IOException {
//        return getETL().load(input, params);
//    }
//
//    public final URI postLoad(URI input, URI output, ObjectMap params) throws IOException {
//        return getETL().postLoad(input, output, params);
//    }


    /**
     * Storage Engines must implement these 2 methods in order to the ETL to be able to write and read from database:
        * getDBWriter: this method returns a valid implementation of a DBWriter to write in the storage engine
        * getDBAdaptor: a implemented instance of the corresponding DBAdaptor is returned to query the database.
     */

    public DBWRITER getDBWriter(String dbName, ObjectMap params) throws StorageManagerException;

    public DBADAPTOR getDBAdaptor(String dbName, ObjectMap params) throws StorageManagerException;


}
