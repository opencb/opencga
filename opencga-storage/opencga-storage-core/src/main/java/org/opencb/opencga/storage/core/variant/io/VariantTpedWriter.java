package org.opencb.opencga.storage.core.variant.io;

import org.apache.commons.lang3.StringUtils;
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
//            if (StringUtils.isEmpty(gtStr) || gtStr.equals(GenotypeClass.UNKNOWN_GENOTYPE)) {
//                gtStr = "0/0";
//            }
            Genotype genotype = new Genotype(gtStr);
            sb.append("\t");
            try {
                switch (genotype.getAllele(0)) {
                    case 0:
                        sb.append(variant.getReference());
                        break;
                    case 1:
                        sb.append(variant.getAlternate());
                        break;
                    default:
                        sb.append(0);
                        break;
                }
            } catch (Exception e) {
                sb.append(0);
            }
            sb.append("\t");
            try {
                switch (genotype.getAllele(1)) {
                    case 0:
                        sb.append(variant.getReference());
                        break;
                    case 1:
                        sb.append(variant.getAlternate());
                        break;
                    default:
                        sb.append(0);
                        break;
                }
            } catch (Exception e) {
                sb.append(0);
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
                throw new UncheckedIOException(e);
            }
        }

        return true;
    }
}
