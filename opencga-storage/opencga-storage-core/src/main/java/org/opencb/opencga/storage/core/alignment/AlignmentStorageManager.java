package org.opencb.opencga.storage.core.alignment;

import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.formats.alignment.io.AlignmentDataWriter;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataReader;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataWriter;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentBamDataReader;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.commons.bioformats.alignment.io.writers.AlignmentJsonDataWriter;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 14/08/14.
 */
public abstract class AlignmentStorageManager implements StorageManager<AlignmentDataReader, AlignmentDataWriter, AlignmentQueryBuilder> {

    public AlignmentStorageManager(Path credentialsPath){

    }

    @Override
    public AlignmentDataWriter getDBSchemaWriter() {
        return null;
    }

    @Override
    public AlignmentDataReader getDBSchemaReader() {
        return null;
    }

    @Override
    abstract public AlignmentDataWriter getDBWriter(Path credentials);

    @Override
    abstract public AlignmentQueryBuilder getDBAdaptor(Path credentials);

    @Override
    abstract public void transform(Path input, Path pedigree, Path output, Map<String, Object> params) throws IOException, FileFormatException;

    @Override
    public void preLoad(Path input, Path output, Map<String, Object> params) throws IOException {

    }

    @Override
    public void load(Path input, Path credentials, Map<String, Object> params) throws IOException {

    }
}
