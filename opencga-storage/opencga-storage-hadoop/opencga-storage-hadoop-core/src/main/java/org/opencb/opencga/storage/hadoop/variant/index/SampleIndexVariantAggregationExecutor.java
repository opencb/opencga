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
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.executors.VariantAggregationExecutor;
import org.opencb.opencga.storage.core.variant.query.executors.accumulators.*;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleVariantIndexEntry;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;
import static org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser.CHROM_DENSITY;

public class SampleIndexVariantAggregationExecutor extends VariantAggregationExecutor {

    private final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private VariantStorageMetadataManager metadataManager;
    private static final Pattern CATEGORICAL_PATTERN = Pattern.compile("^([a-zA-Z][a-zA-Z0-9_.:]+)(\\[[a-zA-Z0-9\\-,:*]+])?(:\\*|:\\d+)?$");

    public static final Set<String> VALID_FACETS = new HashSet<>(Arrays.asList(
            CHROM_DENSITY,
            "chromosome",
            "type",
            "genotype", "gt",
            "consequenceType", "ct",
            "biotype", "bt",
            "clinicalSignificance",
            "mendelianError", "me",
            "length",
            "titv"
    ));


    public SampleIndexVariantAggregationExecutor(VariantStorageMetadataManager metadataManager, SampleIndexDBAdaptor sampleIndexDBAdaptor) {
        this.metadataManager = metadataManager;
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
    }

    @Override
    protected boolean canUseThisExecutor(Query query, QueryOptions options, String facet, List<String> reason) throws Exception {
        if (SampleIndexQueryParser.validSampleIndexQuery(query)) {

            // Check if the query is fully covered
            Query filteredQuery = new Query(query);
            SampleIndexQuery sampleIndexQuery = sampleIndexDBAdaptor.parseSampleIndexQuery(filteredQuery);
            Set<VariantQueryParam> params = VariantQueryUtils.validParams(filteredQuery, true);
            params.remove(VariantQueryParam.STUDY);

            if (!params.isEmpty()) {
                // Query filters not covered
                for (VariantQueryParam param : params) {
                    reason.add("Can't use " + getClass().getSimpleName() + " filtering by \""
                            + param.key() + " : " + filteredQuery.getString(param.key()) + "\"");
                }
                return false;
            }

            SampleIndexSchema schema = sampleIndexQuery.getSchema();
            for (String fieldFacedMulti : facet.split(FACET_SEPARATOR)) {
                for (String fieldFaced : fieldFacedMulti.split(NESTED_FACET_SEPARATOR)) {
                    String key = fieldFaced.split("\\[")[0];
                    // Must contain all keys
                    if (!VALID_FACETS.contains(key)) {
                        if (key.equalsIgnoreCase("depth") || key.equalsIgnoreCase("coverage")) {
                            key = "dp";
                        }
                        if (getIndexField(schema, key) == null) {
                            return false;
                        }
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
            Matcher matcher = CATEGORICAL_PATTERN.matcher(facetField);
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
                case "ct": {
                    IndexField<List<String>> field = schema.getCtIndex().getField();
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> s.getAnnotationIndexEntry() == null
                                    ? Collections.emptyList()
                                    : field.decode(s.getAnnotationIndexEntry().getCtIndex()),
                            fieldKey);
                    break;
                }
                case "bt":
                case "biotype": {
                    IndexField<List<String>> field = schema.getBiotypeIndex().getField();
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> s.getAnnotationIndexEntry() == null
                                    ? Collections.emptyList()
                                    : field.decode(s.getAnnotationIndexEntry().getBtIndex()),
                            fieldKey);
                    break;
                }
                case "clinicalSignificance":
                    thisAccumulator = new CategoricalAccumulator<>(
                            s -> {
                                if (s.getAnnotationIndexEntry() == null || !s.getAnnotationIndexEntry().hasClinical()) {
                                    return Collections.emptyList();
                                }
                                return schema.getClinicalIndexSchema().getClinicalSignificanceField()
                                        .readAndDecode(s.getAnnotationIndexEntry().getClinicalIndex());
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
                    IndexField<String> fileDataIndexField = getIndexField(schema, fieldKey);
                    if (fileDataIndexField == null) {
                        throw new IllegalArgumentException("Unknown faced field '" + facetField + "'");
                    }
                    switch (fileDataIndexField.getType()) {
                        case RANGE_LT:
                        case RANGE_GT:
                            List<Range<Double>> ranges = Range.buildRanges(fileDataIndexField.getConfiguration());
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
            }

            if (accumulator != null) {
                thisAccumulator.setNestedFieldAccumulator(accumulator);
            }
            accumulator = thisAccumulator;
        }
        return accumulator;
    }

    private IndexField<String> getIndexField(SampleIndexSchema schema, String fieldKey) {
        for (IndexField<String> customField : schema.getFileIndex().getCustomFields()) {
            if (customField.getId().equalsIgnoreCase(fieldKey)) {
                return customField;
            }
        }
        for (IndexField<String> customField : schema.getFileIndex().getCustomFields()) {
            if (customField.getKey().equalsIgnoreCase(fieldKey)) {
                return customField;
            }
        }
        return null;
    }
}
