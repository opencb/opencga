package org.opencb.opencga.storage.core;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.datastore.core.ObjectMap;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

/**
 * @author imedina
 * @param <DBWRITER>
 * @param <DBADAPTOR>
 */
public interface StorageManager<DBWRITER, DBADAPTOR> { // READER,

    public void addConfigUri(URI configUri);

//    public DBWRITER getDBSchemaWriter(Path output);
//    public READER getDBSchemaReader(Path input) throws IOException;

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
    public URI extract(URI from, URI to, ObjectMap params);


    public URI preTransform(URI input, ObjectMap params) throws IOException, FileFormatException;

    public URI transform(URI input, URI pedigree, URI output, ObjectMap params) throws IOException, FileFormatException;

    public URI postTransform(URI input, ObjectMap params) throws IOException, FileFormatException;


    public URI preLoad(URI input, URI output, ObjectMap params) throws IOException;

    public URI load(URI input, ObjectMap params) throws IOException;

    public URI postLoad(URI input, URI output, ObjectMap params) throws IOException;


    /**
     * Storage Engines must implement these 2 methods in order to the ETL to be able to write and read from database:
        * getDBWriter: this method returns a valid implementation of a DBWriter to write in the storage engine
        * getDBAdaptor: a implemented instance of the corresponding DBAdaptor is returned to query the database.
     */

    public DBWRITER getDBWriter(String dbName, ObjectMap params);

    public DBADAPTOR getDBAdaptor(String dbName, ObjectMap params);


}
