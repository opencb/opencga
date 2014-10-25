package org.opencb.opencga.storage.core;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.datastore.core.ObjectMap;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

/**
 * Created by jacobo on 14/08/14.
 */
public interface StorageManager<DBWRITER, DBADAPTOR> { // READER,

    public void addPropertiesPath(Path propertiesPath);

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

    public void extract(Path from, Path to);


    public void preTransform(URI input, ObjectMap params) throws IOException, FileFormatException;

    public void transform(Path input, Path pedigree, Path output, ObjectMap params) throws IOException, FileFormatException;

    public void postTransform(URI output, ObjectMap params) throws IOException, FileFormatException;


    public void preLoad(Path input, Path output, ObjectMap params) throws IOException;

    public void load(Path input, Path credentials, ObjectMap params) throws IOException;

    public void postLoad(Path input, Path output, ObjectMap params) throws IOException;


    /**
     * Storage Engines must implement these 2 methods in order to the ETL to be able to write and read from database:
        * getDBWriter: this method returns a valid implementation of a DBWriter to write in the storage engine
        * getDBAdaptor: a implemented instance of the corresponding DBAdaptor is returned to query the database.
     */

    public DBWRITER getDBWriter(Path credentials, String dbName, String fileId);

    public DBADAPTOR getDBAdaptor(String dbName);


}
