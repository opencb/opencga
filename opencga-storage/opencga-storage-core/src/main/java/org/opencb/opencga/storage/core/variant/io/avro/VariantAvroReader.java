package org.opencb.opencga.storage.core.variant.io.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.io.DataReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 06/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAvroReader implements DataReader<Variant> {

    final private File variantsFile;
    final private File metadataFile;
    private DatumReader<VariantAvro> datumReader;
    private DataFileReader<VariantAvro> dataFileReader;

    public VariantAvroReader(File variantsFile, File metadataFile) {
        this.variantsFile = variantsFile;
        this.metadataFile = metadataFile;
    }

    @Override
    public boolean open() {
        datumReader = new SpecificDatumReader<>(VariantAvro.class);
        try {
            dataFileReader = new DataFileReader<>(metadataFile, datumReader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            dataFileReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean pre() {
        return true;
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public List<Variant> read(int batchSize) {
        List<Variant> batch = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize && dataFileReader.hasNext(); i++) {
            batch.add(new Variant(dataFileReader.next()));
        }
        return batch;
    }
}
