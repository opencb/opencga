package org.opencb.opencga.storage.core.variant.transform;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.core.io.avro.AvroEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
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
    private final AvroEncoder<VariantAvro> variantAvroAvroEncoder;
    private boolean includeSrc = false;

    protected final static Logger logger = LoggerFactory.getLogger(VariantAvroTransformTask.class);

    public VariantAvroTransformTask(VariantFactory factory, VariantSource source) {
        this.factory = factory;
        this.source = source;

        this.variantAvroAvroEncoder = new AvroEncoder<>(VariantAvro.getClassSchema());
    }


    @Override
    public List<ByteBuffer> apply(List<String> batch) {
        List<VariantAvro> avros = new ArrayList<>(batch.size());
        logger.debug("Transforming {} lines", batch.size());
        for (String line : batch) {
            if (line.startsWith("#") || line.trim().isEmpty()) {
                continue;
            }
            List<Variant> variants;
            try {
                variants = factory.create(source, line);
            } catch (NotAVariantException e) {
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
                }  catch (Exception e) {
                    logger.error("Error parsing line: {}", line);
                    throw new RuntimeException(e);
                }
            }
        }

        List<ByteBuffer> encoded;
        try {
            encoded = variantAvroAvroEncoder.encode(avros);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return encoded;
    }

    @Override
    public void post() {

    }
}
