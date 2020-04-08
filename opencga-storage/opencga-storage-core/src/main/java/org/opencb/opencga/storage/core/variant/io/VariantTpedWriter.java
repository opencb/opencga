package org.opencb.opencga.storage.core.variant.io;

import org.mortbay.io.RuntimeIOException;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;

import java.io.*;
import java.util.List;

/**
 * Created on 21/11/19.
 *
 * @author Joaquin Tarraga &lt;joaquintarraga@gmail.com&gt;
 */
public class VariantTpedWriter implements DataWriter<Variant> {
    private Writer dataOutputStream;
    private final boolean closeStream;
    private int writtenVariants;

    protected VariantTpedWriter(OutputStream os) {
        this(new OutputStreamWriter(os));
    }

    protected VariantTpedWriter(Writer dataOutputStream) {
        this.dataOutputStream = dataOutputStream;
        this.closeStream = false;
    }

    @Override
    public boolean pre() {
        writtenVariants = 0;
        return true;
    }

    public boolean write(List<Variant> batch) {
        for (Variant variant : batch) {
            write(variant);
        }
        return true;
    }

    public boolean write(Variant variant) {
        StringBuilder sb = new StringBuilder();

        sb.append(variant.getChromosome()).append("\t").append(variant.getId()).append("\t0\t").append(variant.getStart());
        for (SampleEntry sampleEntry : variant.getStudies().get(0).getSamples()) {
            String gtStr = sampleEntry.getData().get(0);
            if (gtStr.equals(GenotypeClass.UNKNOWN_GENOTYPE)) {
                gtStr = "0/0";
            }
            Genotype genotype = new Genotype(gtStr);
            sb.append("\t");
            switch (genotype.getAllele(0)) {
                case 0:
                    sb.append(variant.getReference());
//                    sb.append(1);
                    break;
                case 1:
                    sb.append(variant.getAlternate());
//                    sb.append(2);
                    break;
                default:
                    sb.append(0);
                    break;
            }
            sb.append("\t");
            switch (genotype.getAllele(1)) {
                case 0:
                    sb.append(variant.getReference());
//                    sb.append(1);
                    break;
                case 1:
                    sb.append(variant.getAlternate());
//                    sb.append(2);
                    break;
                default:
                    sb.append(0);
                    break;
            }
        }
        sb.append("\n");

        try {
            dataOutputStream.write(sb.toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        ++writtenVariants;
        return true;
    }

    @Override
    public boolean post() {
        try {
            dataOutputStream.flush();
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean close() {
        if (closeStream) {
            try {
                dataOutputStream.close();
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }

        return true;
    }
}
