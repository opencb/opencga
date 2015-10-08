package org.opencb.opencga.storage.core.variant.transform;

import htsjdk.variant.variantcontext.LazyGenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.opencb.biodata.models.variant.*;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;
import org.opencb.biodata.tools.variant.converter.VariantContextToVariantConverter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;
import org.opencb.hpg.bigdata.core.io.avro.AvroEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created on 01/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAvroTransformTask implements ParallelTaskRunner.Task<String, ByteBuffer> {


    private final VariantFactory factory;
    private final VariantSource source;
    private final AvroEncoder<VariantAvro> encoder;
    private boolean includeSrc = false;

    protected final static Logger logger = LoggerFactory.getLogger(VariantAvroTransformTask.class);
    private final VCFCodec vcfCodec;
    private final VariantContextToVariantConverter converter;
    private final VariantNormalizer normalizer;


    public VariantAvroTransformTask(VariantFactory factory, VariantSource source) {
        this.factory = factory;
        this.source = source;

        this.vcfCodec = null;
        this.converter = null;
        this.normalizer = null;
        this.encoder = null;
    }

    public VariantAvroTransformTask(VCFHeader header, VCFHeaderVersion version, VariantSource source) {
        this.factory = null;
        this.source = source;

        this.vcfCodec = new FullVcfCodec();
        this.vcfCodec.setVCFHeader(header, version);
        this.converter = new VariantContextToVariantConverter(source.getStudyId(), source.getFileId());
        this.normalizer = new VariantNormalizer();
        this.encoder = new AvroEncoder<>(VariantAvro.getClassSchema());
    }


    @Override
    public List<ByteBuffer> apply(List<String> batch) {
        List<VariantAvro> avros = new ArrayList<>(batch.size());
        logger.debug("Transforming {} lines", batch.size());
        if (factory != null) {
            for (String line : batch) {
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }
                List<Variant> variants;
                try {
                    variants = factory.create(source, line);
                } catch (NotAVariantException e1) {
                    variants = Collections.emptyList();
                } catch (Exception e) {
                    logger.error("Error parsing line: {}", line);
                    throw e;
                }
                for (Variant variant : variants) {
                    try {
                        if (!includeSrc) {
                            for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
                                if (variantSourceEntry.getAttributes().containsKey("src")) {
                                    variantSourceEntry.getAttributes().remove("src");
                                }
                            }
                        }
                        avros.add(variant.getImpl());
                    } catch (Exception e) {
                        logger.error("Error parsing line: {}", line);
                        throw new RuntimeException(e);
                    }
                }
            }
        } else {
            List<VariantContext> variantContexts = new ArrayList<>(batch.size());
            for (String line : batch) {
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }
                variantContexts.add(vcfCodec.decode(line));
            }

            List<Variant> variants = converter.apply(variantContexts);

            List<Variant> normalizedVariants = normalizer.apply(variants);

            for (Variant normalizedVariant : normalizedVariants) {
                avros.add(normalizedVariant.getImpl());
            }
        }

        List<ByteBuffer> encoded;
        try {
            encoded = encoder.encode(avros);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return encoded;
    }

    @Override
    public void post() {

    }
}
