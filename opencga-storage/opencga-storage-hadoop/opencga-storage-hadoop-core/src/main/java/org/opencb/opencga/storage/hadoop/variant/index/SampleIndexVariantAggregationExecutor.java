package org.opencb.opencga.storage.hadoop.variant.index;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.executors.VariantAggregationExecutor;
import org.opencb.opencga.storage.core.variant.query.executors.accumulators.*;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;
import static org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser.CHROM_DENSITY;

public class SampleIndexVariantAggregationExecutor extends VariantAggregationExecutor {

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
            "bt",
            "biotype",
            "clinicalSignificance",
            "dp",
            "depth",
            "coverage",
            "qual",
            "filter"
    ));


    public SampleIndexVariantAggregationExecutor(VariantStorageMetadataManager metadataManager, SampleIndexDBAdaptor sampleIndexDBAdaptor) {
        this.metadataManager = metadataManager;
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
    }

    @Override
    protected boolean canUseThisExecutor(Query query, QueryOptions options, String facet) throws Exception {
        if (SampleIndexQueryParser.validSampleIndexQuery(query)) {
            for (String fieldFacedMulti : facet.split(FACET_SEPARATOR)) {
                for (String fieldFaced : fieldFacedMulti.split(NESTED_FACET_SEPARATOR)) {
                    String key = fieldFaced.split("\\[")[0];
                    // Must contain all keys
                    if (!VALID_FACETS.contains(key)) {
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }

    @Override
    protected VariantQueryResult<FacetField> aggregation(Query query, QueryOptions options, String facet) throws Exception {
        StopWatch stopWatch = StopWatch.createStarted();

        List<FieldVariantAccumulator<SampleVariantIndexEntry>> accumulators = createAccumulators(query, facet);
        List<FacetField> fields = new ArrayList<>();

        try (CloseableIterator<SampleVariantIndexEntry> sampleVariantIndexEntryIterator = sampleIndexDBAdaptor.rawIterator(query)) {
            // Init top level fields
            for (FieldVariantAccumulator<SampleVariantIndexEntry> accumulator : accumulators) {
                fields.add(accumulator.createField());
            }

            // Loop
            long numMatches = 0;
            int count = 0;
            while (sampleVariantIndexEntryIterator.hasNext()) {
                count++;
                SampleVariantIndexEntry entry = sampleVariantIndexEntryIterator.next();
                for (int i = 0; i < accumulators.size(); i++) {
                    FieldVariantAccumulator<SampleVariantIndexEntry> accumulator = accumulators.get(i);
                    FacetField field = fields.get(i);
                    accumulator.accumulate(field, entry);
                }
            }
            numMatches += count;

            // Tear down and clean up results.
            for (int i = 0; i < accumulators.size(); i++) {
                FieldVariantAccumulator<SampleVariantIndexEntry> accumulator = accumulators.get(i);
                FacetField field = fields.get(i);
                accumulator.cleanEmptyBuckets(field);
            }

            return new VariantQueryResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), 1, numMatches, Collections.emptyList(),
                    fields, null, SampleIndexVariantQueryExecutor.SAMPLE_INDEX_TABLE_SOURCE);
        }
    }

    private List<FieldVariantAccumulator<SampleVariantIndexEntry>> createAccumulators(Query query, String facet) {
        List<FieldVariantAccumulator<SampleVariantIndexEntry>> list = new ArrayList<>();
        for (String f : facet.split(FACET_SEPARATOR)) {
            list.add(createAccumulator(query, f));
        }
        return list;
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
                            s -> Collections.singletonList(s.getGenotype()), fieldKey);
                    break;
                case "consequenceType":
                case "ct":
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> s.getAnnotationIndexEntry() == null
                                    ? Collections.emptyList()
                                    : AnnotationIndexConverter.getSoNamesFromMask(s.getAnnotationIndexEntry().getCtIndex()),
                            fieldKey);
                    break;
                case "bt":
                case "biotype":
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> s.getAnnotationIndexEntry() == null
                                    ? Collections.emptyList()
                                    : AnnotationIndexConverter.getBiotypesFromMask(s.getAnnotationIndexEntry().getBtIndex()),
                            fieldKey);
                    break;
                case "clinicalSignificance":
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> {
                                if (s.getAnnotationIndexEntry() == null) {
                                    return Collections.emptyList();
                                }
                                return AnnotationIndexConverter.getClinicalsFromMask(s.getAnnotationIndexEntry().getClinicalIndex());
                            },
                            "clinicalSignificance");
                    break;
                case "dp":
                case "depth":
                case "coverage":
                    List<Range<Integer>> dpRanges = Range.buildRanges(
                            Arrays.stream(SampleIndexConfiguration.DP_THRESHOLDS)
                                    .mapToInt(s -> (int) s).boxed()
                                    .collect(Collectors.toList()), 0, null);

                    thisAccumulator = RangeAccumulator.fromIndex(t -> {
                        short fileIndex = t.getFileIndex();
                        return (fileIndex & VariantFileIndexConverter.DP_MASK) >>> VariantFileIndexConverter.DP_SHIFT;
                    }, fieldKey, dpRanges, null);
                    break;
                case "qual":
                    List<Range<Double>> qualRanges = Range.buildRanges(
                            Arrays.stream(SampleIndexConfiguration.QUAL_THRESHOLDS)
                                    .boxed()
                                    .collect(Collectors.toList()), 0.0, null);

                    thisAccumulator = RangeAccumulator.fromIndex(t -> {
                        short fileIndex = t.getFileIndex();
                        return (fileIndex & VariantFileIndexConverter.QUAL_MASK) >>> VariantFileIndexConverter.QUAL_SHIFT;
                    }, fieldKey, qualRanges, null);
                    break;
                case "filter":
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> {
                                short fileIndex = s.getFileIndex();
                                if (IndexUtils.testIndexAny(fileIndex, VariantFileIndexConverter.FILTER_PASS_MASK)) {
                                    return Collections.singletonList(VCFConstants.PASSES_FILTERS_v4);
                                } else {
                                    return Collections.singletonList("other");
                                }
                            },
                            fieldKey);
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
