package org.opencb.opencga.storage.core.variant.index.sample;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.core.IndexUtils;
import org.opencb.opencga.storage.core.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.core.variant.index.sample.query.*;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public abstract class SampleIndexDBAdaptor implements VariantIterable {

    private static Logger logger = LoggerFactory.getLogger(SampleIndexDBAdaptor.class);
    protected final VariantStorageMetadataManager metadataManager;
    protected final SampleIndexSchemaFactory schemaFactory;

    public SampleIndexDBAdaptor(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
        this.schemaFactory = new SampleIndexSchemaFactory(metadataManager);
    }

    public SampleIndexQuery parseSampleIndexQuery(Query query) {
        return newParser().parse(query);
    }

    protected SampleIndexQueryParser newParser() {
        return new SampleIndexQueryParser(metadataManager, schemaFactory);
    }

    public abstract VariantDBIterator iterator(SampleIndexQuery query);

    public abstract VariantDBIterator iterator(SampleIndexQuery query, QueryOptions options);

    public abstract CloseableIterator<SampleIndexEntry> rawIterator(int study, int sample) throws IOException;

    public abstract CloseableIterator<SampleIndexEntry> rawIterator(int study, int sample, Region region) throws IOException;

    public abstract CloseableIterator<SampleIndexEntry> rawIterator(int study, int sample, Region region, SampleIndexSchema schema)
            throws IOException;

    public abstract CloseableIterator<SampleIndexVariant> rawIterator(Query query) throws IOException;

    public abstract CloseableIterator<SampleIndexVariant> rawIterator(SampleIndexQuery query) throws IOException;

    public abstract CloseableIterator<SampleIndexVariant> rawIterator(SampleIndexQuery query, QueryOptions options) throws IOException;

    public abstract long count(SampleIndexQuery query);

    public static void printQuery(SampleAnnotationIndexQuery annotationIndexQuery) {
        logger.info("AnnotationIndex = " + IndexUtils.maskToString(
                annotationIndexQuery.getAnnotationIndexMask(), annotationIndexQuery.getAnnotationIndex()));
        if (!annotationIndexQuery.getBiotypeFilter().isNoOp()) {
            logger.info("Biotype filter  = " + annotationIndexQuery.getBiotypeFilter().toString());
        }
        if (!annotationIndexQuery.getConsequenceTypeFilter().isNoOp()) {
            logger.info("CT filter       = " + annotationIndexQuery.getConsequenceTypeFilter().toString());
        }
        if (!annotationIndexQuery.getTranscriptFlagFilter().isNoOp()) {
            logger.info("Tf filter       = " + annotationIndexQuery.getTranscriptFlagFilter().toString());
        }
        if (!annotationIndexQuery.getCtBtTfFilter().isNoOp()) {
            logger.info("CtBtTf filter     = " + annotationIndexQuery.getCtBtTfFilter().toString());
        }
        if (!annotationIndexQuery.getClinicalFilter().isNoOp()) {
            logger.info("Clinical filter = " + annotationIndexQuery.getClinicalFilter());
        }
        if (!annotationIndexQuery.getPopulationFrequencyFilter().isNoOp()) {
            logger.info("PopFreq filter  = " + annotationIndexQuery.getPopulationFrequencyFilter());
        }
    }

    public static void printQuery(SampleIndexQuery query, Query variantsQuery) {
        printQuery(query);

        List<String> nonCoveredParams = VariantQueryUtils.validParams(variantsQuery, true)
                .stream().map(VariantQueryParam::key).collect(Collectors.toList());
        logger.info("Non covered params : " + nonCoveredParams);
    }

    public static void printQuery(SampleIndexQuery query) {
        printQuery(query.getAnnotationIndexQuery());
        logger.info("Study  : " + query.getStudy());
        if (CollectionUtils.isNotEmpty(query.getLocusQueries())) {
            List<Region> regions = query.getAllRegions();
            if (!regions.isEmpty()) {
                if (regions.size() > 10) {
                    logger.info("Regions  : #" + regions.size()
                            + " [ " + regions.get(0) + " , " + regions.get(1) + " .... , " + regions.get(regions.size() - 1) + " ] ");
                } else {
                    logger.info("Regions  : #" + regions.size() + " " + regions);
                }
            }
            List<Variant> variants = query.getAllVariants();
            if (!variants.isEmpty()) {
                if (variants.size() > 10) {
                    logger.info("Variants  : #" + variants.size()
                            + " [ " + variants.get(0) + " , " + variants.get(1) + " .... , " + variants.get(variants.size() - 1) + "] ");
                } else {
                    logger.info("Variants  : #" + variants.size() + " " + variants);
                }
            }
        }

        Iterator<String> iterator = query.getSamplesMap().keySet().iterator();
        while (iterator.hasNext()) {
            String sample = iterator.next();
            logger.info("  Sample : " + sample);
            printSingleSampleIndexQuery(query.forSample(sample), true);
            if (iterator.hasNext()) {
                logger.info("SampleIndex " + query.getQueryOperation().name());
            }
        }
    }

    public static void printQuery(LocusQuery locusQuery) {
        if (locusQuery != null) {
            Region chunk = locusQuery.getChunkRegion();
            if (chunk.getStart() == 0 && chunk.getEnd() == Integer.MAX_VALUE) {
                logger.info("ChunkRegion: [ " + chunk.getChromosome() + " )");
            } else {
                logger.info("ChunkRegion: [ " + chunk.getChromosome() + ":" + chunk.getStart() + "-" + chunk.getEnd() + " )");
            }
            if (!locusQuery.getRegions().isEmpty()) {
                logger.info("  - Regions: " + locusQuery.getRegions());
            }
            if (!locusQuery.getVariants().isEmpty()) {
                logger.info("  - Variants: " + locusQuery.getVariants());
            }
        }
    }

    public static void printQuery(SingleSampleIndexQuery query) {
        printQuery(query.getAnnotationIndexQuery());
        printSingleSampleIndexQuery(query, false);
    }

    private static void printSingleSampleIndexQuery(SingleSampleIndexQuery query, boolean tab) {
        Iterator<SampleFileIndexQuery> iterator = query.getSampleFileIndexQuery().iterator();
        while (iterator.hasNext()) {
            SampleFileIndexQuery sampleFileIndexQuery = iterator.next();
            for (IndexFieldFilter filter : sampleFileIndexQuery.getFilters()) {
                logger.info((tab ? "      " : "") + "Filter       = " + filter);
            }
            if (iterator.hasNext()) {
                logger.info((tab ? "    " : "") + "FileIndex " + query.getSampleFileIndexQuery().getOperation());
            }
        }
        if (query.hasFatherFilter()) {
            logger.info((tab ? "    " : "") + "FatherFilter       = " + IndexUtils.parentFilterToString(query.getFatherFilter()));
        }
        if (query.hasMotherFilter()) {
            logger.info((tab ? "    " : "") + "MotherFilter       = " + IndexUtils.parentFilterToString(query.getMotherFilter()));
        }
    }

}
