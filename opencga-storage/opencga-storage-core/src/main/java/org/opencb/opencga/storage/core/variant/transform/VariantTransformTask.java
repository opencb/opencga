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
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;
import org.opencb.biodata.tools.variant.converter.VariantContextToVariantConverter;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.io.json.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Created on 25/02/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantTransformTask<T> implements ParallelTaskRunner.Task<String, T> {

    protected final VariantFactory factory;
    protected final VariantSource source;
    protected boolean includeSrc = false;

    protected final Logger logger = LoggerFactory.getLogger(VariantAvroTransformTask.class);
    protected final VCFCodec vcfCodec;
    protected final VariantContextToVariantConverter converter;
    protected final VariantNormalizer normalizer;
    protected final Path outputFileJsonFile;
    protected final VariantGlobalStatsCalculator variantStatsTask;
    protected final AtomicLong htsConvertTime = new AtomicLong(0);
    protected final AtomicLong biodataConvertTime = new AtomicLong(0);
    protected final AtomicLong normTime = new AtomicLong(0);
    protected final List<BiConsumer<String, RuntimeException>> errorHandlers = new ArrayList<>();
    protected boolean failOnError = false;

    public VariantTransformTask(VariantFactory factory,
                                VariantSource source, Path outputFileJsonFile, VariantGlobalStatsCalculator variantStatsTask,
                                boolean includesrc) {
        this.factory = factory;
        this.source = source;
        this.outputFileJsonFile = outputFileJsonFile;
        this.variantStatsTask = variantStatsTask;
        this.includeSrc = includesrc;

        this.vcfCodec = null;
        this.converter = null;
        this.normalizer = null;
    }

    public VariantTransformTask(VCFHeader header, VCFHeaderVersion version,
                                VariantSource source, Path outputFileJsonFile, VariantGlobalStatsCalculator variantStatsTask,
                                boolean includeSrc, boolean generateReferenceBlocks) {
        this.variantStatsTask = variantStatsTask;
        this.factory = null;
        this.source = source;
        this.outputFileJsonFile = outputFileJsonFile;
        this.includeSrc = includeSrc;

        this.vcfCodec = new FullVcfCodec();
        this.vcfCodec.setVCFHeader(header, version);
        this.converter = new VariantContextToVariantConverter(source.getStudyId(), source.getFileId(), source.getSamples());
        this.normalizer = new VariantNormalizer();
        normalizer.setGenerateReferenceBlocks(generateReferenceBlocks);
    }

    @Override
    public void pre() {
        synchronized (variantStatsTask) {
            variantStatsTask.pre();
        }
    }

    @Override
    public List<T> apply(List<String> batch) {
        List<Variant> transformedVariants = new ArrayList<>(batch.size());
        logger.debug("Transforming {} lines", batch.size());
        long curr;
        if (factory != null) {
            for (String line : batch) {
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }
                List<Variant> variants;
                try {
                    curr = System.currentTimeMillis();
                    variants = factory.create(source, line);
                    this.biodataConvertTime.addAndGet(System.currentTimeMillis() - curr);

                    for (Variant variant : variants) {
                        if (!includeSrc) {
                            for (StudyEntry studyEntry : variant.getStudies()) {
                                for (FileEntry fileEntry : studyEntry.getFiles()) {
                                    if (fileEntry.getAttributes().containsKey(VariantVcfFactory.SRC)) {
                                        fileEntry.getAttributes().remove(VariantVcfFactory.SRC);
                                    }
                                }
                            }
                        }
                        transformedVariants.add(variant);
                    }

                    variantStatsTask.apply(variants);

                } catch (NotAVariantException ignore) {
                    variants = Collections.emptyList();
                } catch (RuntimeException e) {
                    onError(e, line);
                }
            }
        } else {
            List<VariantContext> variantContexts = new ArrayList<>(batch.size());
            curr = System.currentTimeMillis();
            for (String line : batch) {
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }
                try {
                    variantContexts.add(vcfCodec.decode(line));
                } catch (RuntimeException e) {
                    onError(e, line);
                }
            }
            this.htsConvertTime.addAndGet(System.currentTimeMillis() - curr);

            curr = System.currentTimeMillis();
            List<Variant> variants = converter.apply(variantContexts);
            this.biodataConvertTime.addAndGet(System.currentTimeMillis() - curr);

            curr = System.currentTimeMillis();
            List<Variant> normalizedVariants = new ArrayList<>((int) (variants.size() * 1.1));
            for (Variant variant : variants) {
                try {
                    normalizedVariants.addAll(normalizer.normalize(Collections.singletonList(variant), true));
                } catch (Exception e) {
                    logger.error("Error parsing variant " + variant);
                    throw new IllegalStateException(e);
                }
            }
            this.normTime.addAndGet(System.currentTimeMillis() - curr);

            variantStatsTask.apply(normalizedVariants);

            transformedVariants.addAll(normalizedVariants);
        }

        return encodeVariants(transformedVariants);
    }

    private void onError(RuntimeException e, String line) {
        logger.error("Error parsing line: {}", line);
        for (BiConsumer<String, RuntimeException> handler : errorHandlers) {
            handler.accept(line, e);
        }
        if (failOnError) {
            throw e;
        }
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
        logger.debug("Time txt2hts: " + this.htsConvertTime.get());
        logger.debug("Time hts2biodata: " + this.biodataConvertTime.get());
        logger.debug("Time normalization: " + this.normTime.get());
    }

    public boolean isIncludeSrc() {
        return includeSrc;
    }

    public VariantTransformTask setIncludeSrc(boolean includeSrc) {
        this.includeSrc = includeSrc;
        return this;
    }

    public VariantTransformTask addMalformedErrorHandler(BiConsumer<String, RuntimeException> handler) {
        errorHandlers.add(handler);
        return this;
    }

    public VariantTransformTask setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    protected abstract List<T> encodeVariants(List<Variant> variants);

}
