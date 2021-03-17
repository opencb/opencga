package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.executors.VariantAggregationExecutor;
import org.opencb.opencga.storage.core.variant.query.executors.accumulators.*;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleVariantIndexEntry;

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
            "genotype", "gt",
            "consequenceType", "ct",
            "biotype", "bt",
            "clinicalSignificance",
            "depth", "dp", "coverage",
            "qual",
            "mendelianError", "me",
            "filter",
            "length",
            "titv"
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

        List<FacetFieldAccumulator<SampleVariantIndexEntry>> accumulators = createAccumulators(query, facet);
        List<FacetField> fields = new ArrayList<>();

        try (CloseableIterator<SampleVariantIndexEntry> sampleVariantIndexEntryIterator = sampleIndexDBAdaptor.rawIterator(query)) {
            // Init top level fields
            for (FacetFieldAccumulator<SampleVariantIndexEntry> accumulator : accumulators) {
                fields.add(accumulator.createField());
            }

            // Loop
            long numMatches = 0;
            int count = 0;
            while (sampleVariantIndexEntryIterator.hasNext()) {
                count++;
                SampleVariantIndexEntry entry = sampleVariantIndexEntryIterator.next();
                for (int i = 0; i < accumulators.size(); i++) {
                    FacetFieldAccumulator<SampleVariantIndexEntry> accumulator = accumulators.get(i);
                    FacetField field = fields.get(i);
                    accumulator.accumulate(field, entry);
                }
            }
            numMatches += count;

            // Tear down and clean up results.
            for (int i = 0; i < accumulators.size(); i++) {
                FacetFieldAccumulator<SampleVariantIndexEntry> accumulator = accumulators.get(i);
                FacetField field = fields.get(i);
                accumulator.evaluate(field);
            }

            return new VariantQueryResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), 1, numMatches, Collections.emptyList(),
                    fields, null, SampleIndexVariantQueryExecutor.SAMPLE_INDEX_TABLE_SOURCE);
        }
    }

    private List<FacetFieldAccumulator<SampleVariantIndexEntry>> createAccumulators(Query query, String facet) {
        List<FacetFieldAccumulator<SampleVariantIndexEntry>> list = new ArrayList<>();
        for (String f : facet.split(FACET_SEPARATOR)) {
            list.add(createAccumulator(query, f));
        }
        return list;
    }

    private FacetFieldAccumulator<SampleVariantIndexEntry> createAccumulator(Query query, String facet) {
        String[] split = facet.split(NESTED_FACET_SEPARATOR);
        FacetFieldAccumulator<SampleVariantIndexEntry> accumulator = null;
        StudyMetadata defaultStudy = VariantQueryParser.getDefaultStudy(query, metadataManager);
        SampleIndexSchema schema = sampleIndexDBAdaptor.getSchema(defaultStudy.getId());

        // Reverse traverse
        for (int i = split.length - 1; i >= 0; i--) {
            String facetField = split[i];
            Matcher matcher = FacetQueryParser.CATEGORICAL_PATTERN.matcher(facetField);
            if (!matcher.find()) {
                throw new VariantQueryException("Malformed aggregation stats query: " + facetField);
            }
            String fieldKey = matcher.group(1);
            String argsStr = matcher.group(2);
            List<String> args;
            if (StringUtils.isNotEmpty(argsStr)) {
                args = Arrays.asList(argsStr.substring(1, argsStr.length() - 1).split(","));
            } else {
                args = Collections.emptyList();
            }
            String stepStr = matcher.group(3);
            if (StringUtils.isNotEmpty(stepStr)) {
                stepStr = stepStr.substring(1);
            }

            if (fieldKey.equalsIgnoreCase("depth") || fieldKey.equalsIgnoreCase("coverage")) {
                fieldKey = "dp";
            }
            FacetFieldAccumulator<SampleVariantIndexEntry> thisAccumulator = null;
            switch (fieldKey) {
                case CHROM_DENSITY:
                    int step;
                    Region region = null;
                    if (args.size() != 1) {
                        throw new VariantQueryException("Malformed aggregation stats query: " + facetField);
                    }
                    if (!StringUtils.isEmpty(stepStr)) {
                        step = Integer.parseInt(stepStr);
                    } else {
                        step = 1000000;
                    }

                    String regionStr = args.get(0);
                    // for (String regionStr : regionsStr.split(",")) {
                    //     regions.add(new Region(regionStr));
                    // }
                    region = new Region(regionStr);
                    query.put(REGION.key(), regionStr);

                    thisAccumulator = new ChromDensityAccumulator<>(metadataManager, region, null, step, s -> s.getVariant().getStart());
                    break;
                case "chromosome":
                    thisAccumulator = new CategoricalAccumulator<>(s -> Collections.singletonList(s.getVariant().getChromosome()),
                            VariantField.CHROMOSOME.fieldName());
                    break;
                case "type":
                    List<VariantType> types = new ArrayList<>();
                    for (String arg : args) {
                        VariantType type = VariantType.valueOf(arg.toUpperCase());
                        types.add(type);
                        types.addAll(Variant.subTypes(type));
                    }
                    thisAccumulator = new VariantTypeAccumulator<>(s -> s.getVariant().getType(), types);
                    break;
                case "titv":
                    thisAccumulator = new RatioAccumulator<>(
                            t -> VariantStats.isTransition(t.getVariant().getReference(), t.getVariant().getAlternate()) ? 1 : 0,
                            t -> VariantStats.isTransversion(t.getVariant().getReference(), t.getVariant().getAlternate()) ? 1 : 0,
                            fieldKey
                    );
                    break;
                case "length":
                    List<Range<Integer>> lengthRanges = Arrays.asList(
                            new Range<>(1, 5),
                            new Range<>(5, 10),
                            new Range<>(10, 20),
                            new Range<>(20, 50),
                            new Range<>(50, null)
                    );

                    thisAccumulator = RangeAccumulator.fromValue(t -> t.getVariant().getLength(), fieldKey, lengthRanges, null);
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
                case "mendelianError":
                case "me":
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> {
                                Integer meCode = s.getMeCode();
                                if (meCode == null) {
                                    return Collections.emptyList();
                                } else {
                                    return Collections.singletonList(meCode.toString());
                                }
                            },
                            fieldKey);
                    break;
                default:
                    for (IndexField<String> fileDataIndexField : schema.getFileIndex().getCustomFields()) {
                        if (fileDataIndexField.getKey().equalsIgnoreCase(fieldKey)) {
                            switch (fileDataIndexField.getType()) {
                                case RANGE:
                                    double[] thresholds = fileDataIndexField.getConfiguration().getThresholds();
                                    List<Range<Double>> ranges = Range.buildRanges(
                                            Arrays.stream(thresholds)
                                                    .boxed()
                                                    .collect(Collectors.toList()), 0.0, null);
                                    thisAccumulator = RangeAccumulator.fromIndex(t -> {
                                        BitBuffer fileIndex = t.getFileIndex();
                                        return fileDataIndexField.read(fileIndex);
                                    }, fieldKey, ranges, null);
                                    break;
                                case CATEGORICAL:
                                    thisAccumulator = new CategoricalAccumulator<>(
                                            s -> {
                                                BitBuffer fileIndex = s.getFileIndex();
                                                String value = fileDataIndexField.readAndDecode(fileIndex);
                                                if (value == null) {
                                                    return Collections.singletonList("other");
                                                } else {
                                                    return Collections.singletonList(value);
                                                }
                                            },
                                            fieldKey);
                                    break;
                                case CATEGORICAL_MULTI_VALUE:
                                    thisAccumulator = new CategoricalAccumulator<>(
                                            s -> {
                                                BitBuffer fileIndex = s.getFileIndex();
                                                String value = fileDataIndexField.readAndDecode(fileIndex);
                                                if (value == null) {
                                                    return Collections.singletonList("other");
                                                } else {
                                                    return Arrays.asList(value.split(","));
                                                }
                                            },
                                            fieldKey);
                                    break;
                                default:
                                    throw new IllegalStateException("Unknown index type " + fileDataIndexField.getType());
                            }
                            break;
                        }
                    }
                    if (thisAccumulator == null) {
                        throw new IllegalArgumentException("Unknown faced field '" + facetField + "'");
                    }
            }

            if (accumulator != null) {
                thisAccumulator.setNestedFieldAccumulator(accumulator);
            }
            accumulator = thisAccumulator;
        }
        return accumulator;
    }
}
