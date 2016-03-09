package org.opencb.opencga.storage.core.variant.transform;

import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.hpg.bigdata.core.io.avro.AvroEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 01/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAvroTransformTask extends VariantTransformTask<ByteBuffer> {

    protected final AvroEncoder<VariantAvro> encoder;

    public VariantAvroTransformTask(VariantFactory factory, VariantSource source, Path outputFileJsonFile, VariantGlobalStatsCalculator variantStatsTask, boolean includesrc) {
        super(factory, source, outputFileJsonFile, variantStatsTask, includesrc);
        this.encoder = new AvroEncoder<>(VariantAvro.getClassSchema());
    }

    public VariantAvroTransformTask(VCFHeader header, VCFHeaderVersion version, VariantSource source, Path outputFileJsonFile, VariantGlobalStatsCalculator variantStatsTask, boolean includeSrc) {
        super(header, version, source, outputFileJsonFile, variantStatsTask, includeSrc);
        this.encoder = new AvroEncoder<>(VariantAvro.getClassSchema());
    }


    @Override
    protected List<ByteBuffer> encodeVariants(List<Variant> variants) {
        List<VariantAvro> avros = new ArrayList<>(variants.size());
        variants.forEach(variant -> avros.add(variant.getImpl()));
        List<ByteBuffer> encoded;
        try {
            encoded = encoder.encode(avros);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return encoded;
    }
}
