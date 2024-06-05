package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.variant.cosmic.CosmicParserCallback;
import org.opencb.biodata.models.sequence.SequenceLocation;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.exceptions.NonStandardCompliantSampleField;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.opencga.core.common.JacksonUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CosmicExtensionTaskCallback implements CosmicParserCallback {

    private RocksDB rdb;
    private VariantNormalizer variantNormalizer;
    private ObjectMapper defaultObjectMapper;

    private static Logger logger = LoggerFactory.getLogger(CosmicExtensionTaskCallback.class);

    private static final String VARIANT_STRING_PATTERN = "([ACGTN]*)|(<CNV[0-9]+>)|(<DUP>)|(<DEL>)|(<INS>)|(<INV>)";

    public CosmicExtensionTaskCallback(RocksDB rdb) {
        this.rdb = rdb;
        this.variantNormalizer = new VariantNormalizer(new VariantNormalizer.VariantNormalizerConfig()
                .setReuseVariants(true)
                .setNormalizeAlleles(true)
                .setDecomposeMNVs(false));
        this.defaultObjectMapper = JacksonUtils.getDefaultObjectMapper();
    }

    @Override
    public boolean processEvidenceEntries(SequenceLocation sequenceLocation, List<EvidenceEntry> evidenceEntries) {
        // Add evidence entries in the RocksDB
        // More than one variant being returned from the normalisation process would mean it's and MNV which has been decomposed
        List<String> normalisedVariantStringList;
        try {
            normalisedVariantStringList = getNormalisedVariantString(sequenceLocation.getChromosome(),
                    sequenceLocation.getStart(), sequenceLocation.getReference(), sequenceLocation.getAlternate());
            if (CollectionUtils.isNotEmpty(normalisedVariantStringList)) {
                for (String normalisedVariantString : normalisedVariantStringList) {
                    rdb.put(normalisedVariantString.getBytes(), defaultObjectMapper.writeValueAsBytes(evidenceEntries));
                }
                return true;
            }
            return false;
        } catch (NonStandardCompliantSampleField | RocksDBException | JsonProcessingException e) {
            logger.warn(StringUtils.join(e.getStackTrace(), "\n"));
            return false;
        }
    }

    protected List<String> getNormalisedVariantString(String chromosome, int start, String reference, String alternate)
            throws NonStandardCompliantSampleField {
        Variant variant = new Variant(chromosome, start, reference, alternate);
        return getNormalisedVariantString(variant);
    }

    protected List<String> getNormalisedVariantString(Variant variant) throws NonStandardCompliantSampleField {
        // Checks no weird characters are part of the reference & alternate alleles
        if (isValid(variant)) {
            List<Variant> normalizedVariantList = variantNormalizer.normalize(Collections.singletonList(variant), true);
            return normalizedVariantList.stream().map(Variant::toString).collect(Collectors.toList());
        } else {
            logger.warn("Variant {} is not valid: skipping it!", variant);
        }

        return Collections.emptyList();
    }

    protected boolean isValid(Variant variant) {
        return (variant.getReference().matches(VARIANT_STRING_PATTERN)
                && (variant.getAlternate().matches(VARIANT_STRING_PATTERN)
                && !variant.getAlternate().equals(variant.getReference())));
    }
}
