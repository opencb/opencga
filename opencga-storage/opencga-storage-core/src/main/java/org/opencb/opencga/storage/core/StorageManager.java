package org.opencb.opencga.storage.core;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.datastore.core.ObjectMap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/**
 * Created by jacobo on 14/08/14.
 */
public interface StorageManager<READER, WRITER, ADAPTOR> {

    public void addPropertiesPath(Path propertiesPath);

    public WRITER getDBSchemaWriter(Path output);
    public READER getDBSchemaReader(Path input);
    public WRITER getDBWriter(Path credentials, String dbName, String fileId);
    public ADAPTOR getDBAdaptor(String dbName);

    public void transform(Path input, Path pedigree, Path output, ObjectMap params) throws IOException, FileFormatException;
    public void preLoad(Path input, Path output, ObjectMap params) throws IOException;
    public void load(Path input, Path credentials, ObjectMap params) throws IOException;
}
