/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.formats.variant.VariantFactory;
import org.opencb.biodata.formats.variant.vcf4.FullVcfCodec;
import org.opencb.biodata.formats.variant.vcf4.VariantVcfFactory;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;
import org.opencb.biodata.models.variant.metadata.VariantFileHeader;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.biodata.tools.variant.stats.VariantSetStatsCalculator;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
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
public abstract class VariantTransformTask<T> implements Task<String, T> {

    protected final VariantFactory factory;
    protected final VariantFileMetadata fileMetadata;
    protected boolean includeSrc = false;

    protected final Logger logger = LoggerFactory.getLogger(VariantAvroTransformTask.class);
    protected final VCFCodec vcfCodec;
    protected final VariantContextToVariantConverter converter;
    protected final VariantNormalizer normalizer;
    protected final VariantSetStatsCalculator variantStatsTask;
    protected final AtomicLong htsConvertTime = new AtomicLong(0);
    protected final AtomicLong biodataConvertTime = new AtomicLong(0);
    protected final AtomicLong normTime = new AtomicLong(0);
    protected final List<BiConsumer<String, RuntimeException>> errorHandlers = new ArrayList<>();
    protected boolean failOnError = false;
    private VariantStudyMetadata metadata;

    public VariantTransformTask(VariantFactory factory,
                                String studyId, VariantFileMetadata fileMetadata,
                                VariantSetStatsCalculator variantStatsTask,
                                boolean includesrc, boolean generateReferenceBlocks) {
        this.factory = factory;
        this.fileMetadata = fileMetadata;
        this.metadata = fileMetadata.toVariantStudyMetadata(studyId);
        this.variantStatsTask = variantStatsTask;
        this.includeSrc = includesrc;

        this.vcfCodec = null;
        this.converter = null;
        this.normalizer = new VariantNormalizer(true, true, false);
        normalizer.setGenerateReferenceBlocks(generateReferenceBlocks);
    }

    public VariantTransformTask(VCFHeader header, VCFHeaderVersion version,
                                String studyId, VariantFileMetadata fileMetadata,
                                VariantSetStatsCalculator variantStatsTask,
                                boolean includeSrc, boolean generateReferenceBlocks) {
        this.variantStatsTask = variantStatsTask;
        this.factory = null;
        this.fileMetadata = fileMetadata;
        this.metadata = fileMetadata.toVariantStudyMetadata(studyId);
        this.includeSrc = includeSrc;

        this.vcfCodec = new FullVcfCodec();
        this.vcfCodec.setVCFHeader(header, version);
        this.converter = new VariantContextToVariantConverter(studyId, fileMetadata.getId(), fileMetadata.getSampleIds());
        this.normalizer = new VariantNormalizer(true, true, false);
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
                    variants = factory.create(metadata, line);
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
                    }

                    List<Variant> normalizedVariants = normalize(variants);

                    variantStatsTask.apply(normalizedVariants);

                    transformedVariants.addAll(normalizedVariants);

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

            List<Variant> normalizedVariants = normalize(variants);

            variantStatsTask.apply(normalizedVariants);

            transformedVariants.addAll(normalizedVariants);
        }

        return encodeVariants(transformedVariants);
    }

    public List<Variant> normalize(List<Variant> variants) {
        long curr;
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
        return normalizedVariants;
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
        logger.debug("Time txt2hts: " + this.htsConvertTime.get());
        logger.debug("Time hts2biodata: " + this.biodataConvertTime.get());
        logger.debug("Time normalization: " + this.normTime.get());
    }

    public static void writeVariantFileMetadata(VariantFileMetadata fileMetadata, OutputStream outputMetadataStream) {
        ObjectMapper jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        try {
            jsonObjectMapper.writeValue(outputMetadataStream, fileMetadata.getImpl());
            outputMetadataStream.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public boolean isIncludeSrc() {
        return includeSrc;
    }

    public VariantTransformTask<T> setIncludeSrc(boolean includeSrc) {
        this.includeSrc = includeSrc;
        return this;
    }

    public VariantTransformTask<T> addMalformedErrorHandler(BiConsumer<String, RuntimeException> handler) {
        errorHandlers.add(handler);
        return this;
    }

    public VariantTransformTask<T> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    public VariantTransformTask<T> configureNormalizer(VariantFileHeader header) {
        normalizer.configure(header);
        return this;
    }

    protected abstract List<T> encodeVariants(List<Variant> variants);

}
