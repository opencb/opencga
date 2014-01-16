package org.opencb.opencga.storage.variant;

import java.io.IOException;
import java.util.List;
import org.opencb.commons.bioformats.pedigree.io.readers.PedDataReader;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.vcf4.VcfRecord;
import org.opencb.commons.bioformats.variant.vcf4.io.VariantDBWriter;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantDataReader;
import org.opencb.commons.bioformats.variant.vcf4.io.writers.index.VariantDataWriter;
import org.opencb.commons.io.DataWriter;
import org.opencb.variant.lib.runners.VariantRunner;

/**
 * @author Alejandro Aleman Ramos
 * @author Cristina Yenyxe Gonzalez Garcia
 */
public class VariantIndexRunner extends VariantRunner {

    public VariantIndexRunner(VariantStudy study, VariantDataReader reader, PedDataReader pedReader, DataWriter writer, VariantRunner prev) {
        super(study, reader, pedReader, writer, prev);
    }

    public VariantIndexRunner(VariantStudy study, VariantDataReader reader, PedDataReader pedReader, DataWriter writer) {
        super(study, reader, pedReader, writer);
    }

    @Override
    public List<VcfRecord> apply(List<VcfRecord> batch) throws IOException {
        if (writer != null) {
            ((VariantDataWriter) writer).writeBatch(batch);
        }
        return batch;

    }

    @Override
    public void post() throws IOException {
        if (writer instanceof VariantDBWriter) {
            ((VariantDBWriter) writer).writeStudy(getStudy());
        }
    }
    
}
