package org.opencb.opencga.storage.core;

import org.opencb.biodata.formats.io.FileFormatException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/**
 * Created by jacobo on 14/08/14.
 */
public interface StorageManager<READER, WRITER, ADAPTOR> {

    public WRITER getDBSchemaWriter(Path output);
    public READER getDBSchemaReader(Path input);
    public WRITER getDBWriter(Path credentials, String fileId);
    public ADAPTOR getDBAdaptor(Path credentials);

    public void transform(Path input, Path pedigree, Path output, Map<String, Object> params) throws IOException, FileFormatException;
    public void preLoad(Path input, Path output, Map<String, Object> params) throws IOException;
    public void load(Path input, Path credentials, Map<String, Object> params) throws IOException;
}
