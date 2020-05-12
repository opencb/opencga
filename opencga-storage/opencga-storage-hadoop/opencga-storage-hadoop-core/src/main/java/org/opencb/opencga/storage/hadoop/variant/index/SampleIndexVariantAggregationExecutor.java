package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.query.executors.AbstractLocalVariantAggregationExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleVariantIndexEntry;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;
import static org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser.CHROM_DENSITY;

public class SampleIndexVariantAggregationExecutor extends AbstractLocalVariantAggregationExecutor {

    private final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private VariantStorageMetadataManager metadataManager;
    public static final Set<String> VALID_FACETS = new HashSet<>(Arrays.asList(
            CHROM_DENSITY,
            "chromosome",
            "type",
            "genotype",
            "gt",
            "consequenceType",
            "ct",
            "biotype",
            "clinicalSignificance"
    ));


    public SampleIndexVariantAggregationExecutor(VariantStorageMetadataManager metadataManager, SampleIndexDBAdaptor sampleIndexDBAdaptor) {
        this.metadataManager = metadataManager;
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
    }

    @Override
    protected boolean canUseThisExecutor(Query query, QueryOptions options, String facet) throws Exception {
        if (SampleIndexQueryParser.validSampleIndexQuery(query)) {
            String[] split = facet.split(NESTED_FACET_SEPARATOR);
            for (String fieldFaced : split) {
                String key = fieldFaced.split("\\[")[0];
                // Must contain all keys
                if (!VALID_FACETS.contains(key)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    protected VariantQueryResult<FacetField> aggregation(Query query, QueryOptions options, String facet) throws Exception {
        StopWatch stopWatch = StopWatch.createStarted();

        FieldVariantAccumulator<SampleVariantIndexEntry> accumulator = createAccumulator(query, facet);

        FacetField topLevelField;
        try (CloseableIterator<SampleVariantIndexEntry> sampleVariantIndexEntryIterator = sampleIndexDBAdaptor.rawIterator(query)) {
            topLevelField = accumulator.createField();

            long numMatches = 0;
            int count = 0;
            while (sampleVariantIndexEntryIterator.hasNext()) {
                count++;
                accumulator.accumulate(topLevelField, sampleVariantIndexEntryIterator.next());
            }
            numMatches += count;

            accumulator.cleanEmptyBuckets(topLevelField);

            return new VariantQueryResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), 1, numMatches, Collections.emptyList(),
                    Collections.singletonList(topLevelField), null, SampleIndexVariantQueryExecutor.SAMPLE_INDEX_TABLE_SOURCE);
        }
    }

    private FieldVariantAccumulator<SampleVariantIndexEntry> createAccumulator(Query query, String facet) {
        String[] split = facet.split(NESTED_FACET_SEPARATOR);
        FieldVariantAccumulator<SampleVariantIndexEntry> accumulator = null;
        // Reverse traverse
        for (int i = split.length - 1; i >= 0; i--) {
            String facetField = split[i];
            String fieldKey = facetField.split("\\[")[0];

            final FieldVariantAccumulator<SampleVariantIndexEntry> thisAccumulator;
            switch (fieldKey) {
                case CHROM_DENSITY:
                    int step;
                    Region region = null;
                    Matcher matcher = CHROM_DENSITY_PATTERN.matcher(facetField);
                    if (matcher.matches()) {
                        String regionStr = matcher.group(1);
                        String stepStr = matcher.group(3);

                        if (!StringUtils.isEmpty(stepStr)) {
                            step = Integer.parseInt(stepStr);
                        } else {
                            step = 1000000;
                        }

                        // for (String regionStr : regionsStr.split(",")) {
                        //     regions.add(new Region(regionStr));
                        // }
                        region = new Region(regionStr);
                        query.put(REGION.key(), regionStr);
                    } else {
                        throw new VariantQueryException("Malformed aggregation stats query: " + facetField);
                    }
                    thisAccumulator = new ChromDensityAccumulator<>(metadataManager, region, null, step, s -> s.getVariant().getStart());
                    break;
                case "chromosome":
                    thisAccumulator = new CategoricalAccumulator<>(s -> Collections.singletonList(s.getVariant().getChromosome()),
                            VariantField.CHROMOSOME.fieldName());
                    break;
                case "type":
                    thisAccumulator = new VariantTypeAccumulator<>(s -> s.getVariant().getType());
                    break;
                case "genotype":
                case "gt":
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> Collections.singletonList(s.getGenotype()), "genotype");
                    break;
                case "consequenceType":
                case "ct":
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> AnnotationIndexConverter.getSoNamesFromMask(s.getAnnotationIndexEntry().getCtIndex()),
                            "consequenceType");
                    break;
                case "biotype":
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> AnnotationIndexConverter.getBiotypesFromMask(s.getAnnotationIndexEntry().getBtIndex()),
                            "biotype");
                    break;
                case "clinicalSignificance":
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> {
                                List<ClinicalSignificance> values = AnnotationIndexConverter
                                        .getClinicalsFromMask(s.getAnnotationIndexEntry().getClinicalIndex());
                                if (values.isEmpty()) {
                                    return Collections.emptyList();
                                } else {
                                    return values.stream().map(Objects::toString).collect(Collectors.toList());
                                }
                            },
                            "clinicalSignificance");
                    break;
                default:
                    throw new IllegalArgumentException("Unknown faced field '" + facetField + "'");
            }

            if (accumulator != null) {
                thisAccumulator.setNestedFieldAccumulator(accumulator);
            }
            accumulator = thisAccumulator;
        }
        return accumulator;
    }
}
