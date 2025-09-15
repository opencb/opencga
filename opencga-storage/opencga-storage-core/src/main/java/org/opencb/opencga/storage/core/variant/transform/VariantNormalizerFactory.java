package org.opencb.opencga.storage.core.variant.transform;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.VariantReferenceBlockCreatorTask;
import org.opencb.biodata.tools.variant.VariantSorterTask;
import org.opencb.biodata.tools.variant.normalizer.extensions.VariantNormalizerExtensionFactory;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.*;

public class VariantNormalizerFactory {

    private static Logger logger = LoggerFactory.getLogger(VariantNormalizerFactory.class);
    private final boolean generateReferenceBlocks;
    private Collection<String> enabledExtensions;
    private final String referenceGenome;

    public VariantNormalizerFactory(ObjectMap options) {
        if (options == null) {
            options = new ObjectMap();
        }
        generateReferenceBlocks = options.getBoolean(GVCF.key(), false);
        referenceGenome = options.getString(NORMALIZATION_REFERENCE_GENOME.key());
        enabledExtensions = options.getAsStringList(NORMALIZATION_EXTENSIONS.key());

        if (CollectionUtils.isEmpty(enabledExtensions)) {
            // Undefined, use default
            enabledExtensions = NORMALIZATION_EXTENSIONS.defaultValue();
        }
        if (enabledExtensions.size() == 1 && enabledExtensions.contains(ParamConstants.ALL)) {
            // Use all extensions
            enabledExtensions = NORMALIZATION_EXTENSIONS.defaultValue();
        }
        if (enabledExtensions.size() == 1 && enabledExtensions.contains(ParamConstants.NONE)) {
            enabledExtensions = Collections.emptyList();
        }
    }

    public Task<Variant, Variant> newNormalizer(VariantFileMetadata metadata) throws StorageEngineException {
        logger.info("Normalizing variants. Generate reference blocks: {}", generateReferenceBlocks);
        VariantNormalizer.VariantNormalizerConfig normalizerConfig = new VariantNormalizer.VariantNormalizerConfig()
                .setReuseVariants(true)
                .setNormalizeAlleles(true)
                .setDecomposeMNVs(false)
                .setGenerateReferenceBlocks(generateReferenceBlocks);
        if (StringUtils.isNotEmpty(referenceGenome)) {
            try {
                logger.info("Enable left alignment with reference genome file '{}'", referenceGenome);
                normalizerConfig.enableLeftAlign(referenceGenome);
            } catch (IOException e) {
                throw StorageEngineException.ioException(e);
            }
        }

        Task<Variant, Variant> normalizer = new VariantNormalizer(normalizerConfig)
                .configure(metadata.getHeader());
        if (generateReferenceBlocks) {
            normalizer = normalizer
                    .then(new VariantSorterTask(100)) // Sort before generating reference blocks
                    .then(new VariantReferenceBlockCreatorTask(metadata.getHeader()));
        }
        if (enabledExtensions.isEmpty()) {
            logger.info("Skip normalization extensions");
        } else {
            logger.info("Enable normalization extensions: {}", enabledExtensions);
            VariantNormalizerExtensionFactory extensionFactory = new VariantNormalizerExtensionFactory(new HashSet<>(enabledExtensions));
            Task<Variant, Variant> extension = extensionFactory.buildExtensions(metadata);
            if (extension == null) {
                logger.info("No normalization extensions can be used.");
            } else {
                normalizer = normalizer.then(extension);
            }
        }

        return normalizer;
    }
}
