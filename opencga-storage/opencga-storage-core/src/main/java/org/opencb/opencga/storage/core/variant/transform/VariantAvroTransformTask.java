package org.opencb.opencga.storage.core.variant.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.formats.variant.vcf4.FullVcfCodec;
import org.opencb.biodata.models.variant.*;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;
import org.opencb.biodata.tools.variant.converter.VariantContextToVariantConverter;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.core.io.avro.AvroEncoder;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.io.json.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created on 01/10/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAvroTransformTask implements ParallelTaskRunner.Task<String, ByteBuffer> {

    private final VariantFactory factory;
    private final VariantSource source;
    private final AvroEncoder<VariantAvro> encoder;
    private boolean includeSrc = false;

    private final VCFCodec vcfCodec;
    private final VariantContextToVariantConverter converter;
    private final VariantNormalizer normalizer;
    private final Path outputFileJsonFile;
    private final VariantGlobalStatsCalculator variantStatsTask;

    protected final Logger logger = LoggerFactory.getLogger(VariantAvroTransformTask.class);

    public VariantAvroTransformTask(VariantFactory factory,
                                    VariantSource source, Path outputFileJsonFile, VariantGlobalStatsCalculator variantStatsTask) {
        this.factory = factory;
        this.source = source;
        this.outputFileJsonFile = outputFileJsonFile;
        this.variantStatsTask = variantStatsTask;

        this.vcfCodec = null;
        this.converter = null;
        this.normalizer = null;
        this.encoder = new AvroEncoder<>(VariantAvro.getClassSchema());
    }

    public VariantAvroTransformTask(VCFHeader header, VCFHeaderVersion version,
                                    VariantSource source, Path outputFileJsonFile, VariantGlobalStatsCalculator variantStatsTask) {
        this.variantStatsTask = variantStatsTask;
        this.factory = null;
        this.source = source;
        this.outputFileJsonFile = outputFileJsonFile;

        this.vcfCodec = new FullVcfCodec();
        this.vcfCodec.setVCFHeader(header, version);
        this.converter = new VariantContextToVariantConverter(source.getStudyId(), source.getFileId(), source.getSamples());
        this.normalizer = new VariantNormalizer();
        this.encoder = new AvroEncoder<>(VariantAvro.getClassSchema());
    }

    @Override
    public void pre() {
        synchronized (variantStatsTask) {
            variantStatsTask.pre();
        }
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
                            for (StudyEntry studyEntry : variant.getStudies()) {
                                for (FileEntry fileEntry : studyEntry.getFiles()) {
                                    if (fileEntry.getAttributes().containsKey(VariantVcfFactory.SRC)) {
                                        fileEntry.getAttributes().remove(VariantVcfFactory.SRC);
                                    }
                                }
                            }
                        }
                        avros.add(variant.getImpl());
                    } catch (Exception e) {
                        logger.error("Error parsing line: {}", line);
                        throw new RuntimeException(e);
                    }
                }
                variantStatsTask.apply(variants);
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
            variantStatsTask.apply(normalizedVariants);

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
        synchronized (variantStatsTask) {
            variantStatsTask.post();
        }
        ObjectMapper jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);

        ObjectWriter variantSourceObjectWriter = jsonObjectMapper.writerFor(VariantSource.class);
        try {
            String sourceJsonString = variantSourceObjectWriter.writeValueAsString(source);
            StringDataWriter.write(outputFileJsonFile, Collections.singletonList(sourceJsonString));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
