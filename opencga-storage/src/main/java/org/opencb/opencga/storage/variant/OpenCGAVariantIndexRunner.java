package org.opencb.opencga.storage.variant;

import org.opencb.commons.bioformats.pedigree.io.readers.PedDataReader;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.vcf4.VcfRecord;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantDataReader;
import org.opencb.commons.bioformats.variant.vcf4.io.writers.index.VariantDataWriter;
import org.opencb.commons.io.DataWriter;
import org.opencb.variant.lib.runners.VariantRunner;

import java.io.IOException;
import java.util.List;

/**
 * Created by aaleman on 12/9/13.
 */
//public class OpenCGAVariantIndexRunner extends Runner<VariantDataReader, DataWriter, VcfRecord> {
public class OpenCGAVariantIndexRunner extends VariantRunner {

    private PedDataReader pedReader;
    private VariantStudy study;

    public OpenCGAVariantIndexRunner(VariantStudy study, VariantDataReader reader, PedDataReader pedReader, DataWriter writer, VariantRunner prev) {
        super(study, reader, pedReader, writer, prev);
        this.pedReader = pedReader;
        this.study = study;
    }

    public OpenCGAVariantIndexRunner(VariantStudy study, VariantDataReader reader, PedDataReader pedReader, DataWriter writer) {
        super(study, reader, pedReader, writer);
        this.pedReader = pedReader;
        this.study = study;
    }

    public VariantStudy getStudy() {
        return study;
    }

    public void setStudy(VariantStudy study) {
        this.study = study;
    }

    @Override
    public List<VcfRecord> apply(List<VcfRecord> batch) throws IOException {

        if (writer != null) {
            ((VariantDataWriter) writer).writeBatch(batch);
        }
        return batch;

    }

    @Override
    public void run() throws IOException {
        List<VcfRecord> batch;

        reader.open();
        reader.pre();

        if (pedReader != null) {
            pedReader.open();
            study.setPedigree(pedReader.read());
            pedReader.close();
        }
        study.addMetadata("variant_file_header", reader.getHeader());
        study.setSamples(reader.getSampleNames());

        this.writerOpen();
        this.writerPre();

        this.launchPre();

        batch = reader.read(batchSize);
        while (!batch.isEmpty()) {

            batch = this.launch(batch);
            batch.clear();
            batch = reader.read(batchSize);

        }

        this.launchPost();

        reader.post();
        reader.close();

        this.writerPost();
        this.writerClose();

    }


}
