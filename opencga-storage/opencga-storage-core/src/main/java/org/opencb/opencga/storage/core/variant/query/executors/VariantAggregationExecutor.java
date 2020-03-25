package org.opencb.opencga.storage.core.variant.query.executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.search.VariantSearchUtils;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser.CHROM_DENSITY;

/**
 * Created on 10/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAggregationExecutor {

    private final VariantSearchManager searchManager;
    private final String dbName;
    private final VariantIterable iterable;
    private final VariantStorageMetadataManager metadataManager;
    private Logger logger = LoggerFactory.getLogger(VariantAggregationExecutor.class);
    public static final Pattern CHROM_DENSITY_PATTERN = Pattern.compile("^" + CHROM_DENSITY + "\\[([a-zA-Z0-9:\\-,*]+)](:(\\d+))?$");
    public static final String NESTED_FACET_SEPARATOR = ">>"; // FacetQueryParser.NESTED_FACET_SEPARATOR
    private static final Set<String> ACCEPTED_CHROM_DENSITY_NESTED = new HashSet<>(Arrays.asList("type"));

    public VariantAggregationExecutor(VariantSearchManager searchManager, String dbName,
                                      VariantIterable iterable,
                                      VariantStorageMetadataManager metadataManager) {
        this.searchManager = searchManager;
        this.dbName = dbName;
        this.iterable = iterable;
        this.metadataManager = metadataManager;
    }

    /**
     * Fetch facet (i.e., counts) resulting of executing the query in the database.
     *
     * @param query          Query to be executed in the database to filter variants
     * @param options        Query modifiers, accepted values are: facet fields and facet ranges
     * @return               A FacetedQueryResult with the result of the query
     */
    public DataResult<FacetField> facet(Query query, QueryOptions options) {
        if (query == null) {
            query = new Query();
        }
        if (options == null) {
            options = new QueryOptions();
        }

        String facet = options.getString(QueryOptions.FACET);
        try {
            if (VariantSearchUtils.isQueryCovered(query)) {
                return searchManager.facetedQuery(dbName, query, options);
            } else if (isPureChromDensityFacet(facet)) {
                return chromDensityAggregation(query, options);
            } else {
                throw new VariantQueryException("Unsupported aggregated stats query. "
                        + "Aggregate by " + CHROM_DENSITY + " or remove sample filters");
            }
        } catch (IOException | SolrException | VariantSearchException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    protected boolean isPureChromDensityFacet(String facet) {
        boolean isChromDensity = facet != null
                && facet.startsWith(CHROM_DENSITY)
                && !facet.contains(FacetQueryParser.FACET_SEPARATOR);
        if (isChromDensity && facet.contains(NESTED_FACET_SEPARATOR)) {
            String[] split = facet.split(NESTED_FACET_SEPARATOR);
            if (split.length == 1) {
                return true;
            } else if (split.length == 2) {
                // Check accepted nested fields
                return ACCEPTED_CHROM_DENSITY_NESTED.contains(split[1]);
            }
            return false;
        } else {
            return isChromDensity;
        }
    }

    private VariantQueryException invalidNestedField(String nestedFieldName) {
        return new VariantQueryException("Unable to calculate " + CHROM_DENSITY + " with nested field " + nestedFieldName);
    }

    protected DataResult<FacetField> chromDensityAggregation(Query query, QueryOptions options) {
        StopWatch stopWatch = StopWatch.createStarted();
        String facet = options.getString(QueryOptions.FACET);

        String[] split = facet.split(NESTED_FACET_SEPARATOR);
        String chromDensityFacet = split[0];
        FieldVariantAccumulator nestedFieldAccumulator;
        if (split.length == 2) {
            String nestedFieldName;
            nestedFieldName = split[1];
            if (!ACCEPTED_CHROM_DENSITY_NESTED.contains(nestedFieldName)) {
                throw invalidNestedField(nestedFieldName);
            }
            switch (nestedFieldName) {
                case "type":
                    nestedFieldAccumulator = new VariantTypeAccumulator();
                    break;
                default:
                    throw invalidNestedField(nestedFieldName);
            }
        } else {
            nestedFieldAccumulator = null;
        }

        int step;
        List<Region> regions = new LinkedList<>();
        Matcher matcher = CHROM_DENSITY_PATTERN.matcher(chromDensityFacet);
        if (matcher.matches()) {
            String regionsStr = matcher.group(1);
            String stepStr = matcher.group(3);

            if (!StringUtils.isEmpty(stepStr)) {
                step = Integer.valueOf(stepStr);
            } else {
                step = 1000000;
            }

            for (String regionStr : regionsStr.split(",")) {
                regions.add(new Region(regionStr));
            }
        } else {
            throw new VariantQueryException("Malformed aggregation stats query: " + chromDensityFacet);
        }

        if (regions.isEmpty()) {
            throw new VariantQueryException("Unable to calculate aggregated stats query without a region or gene");
        }

        List<FacetField.Bucket> regionBuckets = new ArrayList<>(regions.size());
        long numMatches = 0;
        for (Region region : regions) {
            Query regionQuery = new Query(query).append(VariantQueryParam.REGION.key(), region);
            VariantDBIterator iterator = iterable.iterator(
                    regionQuery,
                    new QueryOptions()
                            .append(QueryOptions.INCLUDE, VariantField.ID)
                            .append(QueryOptions.SORT, true));

            ChromDensityAccumulator chromDensityAccumulator = new ChromDensityAccumulator(region, nestedFieldAccumulator, step);

            logger.info("Query : " + regionQuery.toJson());

            FacetField regionField = chromDensityAccumulator.createField();

            int count = 0;
            while (iterator.hasNext()) {
                count++;
                chromDensityAccumulator.accumulate(regionField, iterator.next());
            }
            numMatches += count;

            chromDensityAccumulator.cleanEmptyBuckets(regionField);
            regionBuckets.add(new FacetField.Bucket(region.getChromosome(), count, Collections.singletonList(regionField)));
        }

        FacetField field = new FacetField(
                CHROM_DENSITY,
                regionBuckets.size(),
                regionBuckets);
        return new DataResult<>(((int) stopWatch.getTime(TimeUnit.MILLISECONDS)), Collections.emptyList(), 1,
                Collections.singletonList(field), numMatches);
    }

    private interface FieldVariantAccumulator {
        /**
         * Get field name.
         * @return Field name
         */
        String getName();

        /**
         * Prepare (if required) the list of buckets for this field.
         * @return predefined list of buckets.
         */
        default FacetField createField() {
            return new FacetField(getName(), 0, prepareBuckets());
        }

        /**
         * Prepare (if required) the list of buckets for this field.
         * @return predefined list of buckets.
         */
        List<FacetField.Bucket> prepareBuckets();

        default void cleanEmptyBuckets(FacetField field) {
            field.getBuckets().removeIf(bucket -> bucket.getCount() == 0);
        }

        /**
         * Accumulate variant in the given field.
         * @param field   Field
         * @param variant Variant
         */
        void accumulate(FacetField field, Variant variant);
    }

    private final class ChromDensityAccumulator implements FieldVariantAccumulator {
        private final Region region;
        private final FieldVariantAccumulator nestedFieldAccumulator;
        private final int step;
        private final int numSteps;

        private ChromDensityAccumulator(Region region, FieldVariantAccumulator nestedFieldAccumulator, int step) {
            this.region = region;
            this.nestedFieldAccumulator = nestedFieldAccumulator;
            this.step = step;

            if (region.getEnd() == Integer.MAX_VALUE) {
                for (Integer studyId : metadataManager.getStudyIds()) {
                    StudyMetadata studyMetadata = metadataManager.getStudyMetadata(studyId);
                    VariantFileHeaderComplexLine contig = studyMetadata.getVariantHeaderLine("contig", region.getChromosome());
                    if (contig == null) {
                        contig = studyMetadata.getVariantHeaderLine("contig", "chr" + region.getChromosome());
                    }
                    if (contig != null) {
                        String length = contig.getGenericFields().get("length");
                        if (StringUtils.isNotEmpty(length) && StringUtils.isNumeric(length)) {
                            region.setEnd(Integer.parseInt(length));
                            break;
                        }
                    }
                }
            }
            if (region.getStart() == 0) {
                region.setStart(1);
            }

            int regionLength = region.getEnd() - region.getStart();
            if (regionLength != Integer.MAX_VALUE) {
                regionLength++;
            }
            numSteps = regionLength / step + 1;
        }

        @Override
        public String getName() {
            return VariantField.START.fieldName();
        }

        @Override
        public FacetField createField() {
            return new FacetField(VariantField.START.fieldName(), 0,
                    prepareBuckets())
                    .setStart(region.getStart())
                    .setEnd(region.getEnd())
                    .setStep(step);
        }

        @Override
        public List<FacetField.Bucket> prepareBuckets() {
            List<FacetField.Bucket> valueBuckets = new ArrayList<>(numSteps);
            for (int i = 0; i < numSteps; i++) {
                FacetField.Bucket bucket = new FacetField.Bucket(String.valueOf(i * step + region.getStart()), 0, null);
                if (nestedFieldAccumulator != null) {
                    bucket.setFacetFields(Collections.singletonList(nestedFieldAccumulator.createField()));
                }
                valueBuckets.add(bucket);
            }
            return valueBuckets;
        }

        @Override
        public void accumulate(FacetField field, Variant variant) {
            int idx = (variant.getStart() - region.getStart()) / step;
            if (idx < numSteps) {
                field.addCount(1);
                FacetField.Bucket bucket = field.getBuckets().get(idx);
                bucket.addCount(1);
                if (nestedFieldAccumulator != null) {
                    nestedFieldAccumulator.accumulate(bucket.getFacetFields().get(0), variant);
                }
            }
        }
        @Override
        public void cleanEmptyBuckets(FacetField field) {
            field.getBuckets().removeIf(bucket -> bucket.getCount() == 0);
            if (nestedFieldAccumulator != null) {
                for (FacetField.Bucket bucket : field.getBuckets()) {
                    nestedFieldAccumulator.cleanEmptyBuckets(bucket.getFacetFields().get(0));
                }
            }
        }
    }

    private final class VariantTypeAccumulator implements FieldVariantAccumulator {

        private VariantTypeAccumulator() {
            // TODO: Accept subset of variant type
        }

        @Override
        public String getName() {
            return "type";
        }

        @Override
        public List<FacetField.Bucket> prepareBuckets() {
            List<FacetField.Bucket> buckets = new ArrayList<>(VariantType.values().length);
            for (VariantType variantType : VariantType.values()) {
                buckets.add(new FacetField.Bucket(variantType.name(), 0, null));
            }
            return buckets;
        }

        @Override
        public void accumulate(FacetField field, Variant variant) {
            field.addCount(1);
            field.getBuckets().get(variant.getType().ordinal()).addCount(1);
        }
    }

}
