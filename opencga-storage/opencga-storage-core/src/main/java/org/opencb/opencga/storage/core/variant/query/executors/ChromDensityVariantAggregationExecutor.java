package org.opencb.opencga.storage.core.variant.query.executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser.CHROM_DENSITY;

public class ChromDensityVariantAggregationExecutor extends AbstractLocalVariantAggregationExecutor {

    protected static final Set<String> ACCEPTED_CHROM_DENSITY_NESTED = new HashSet<>(Arrays.asList("type"));

    private final VariantIterable iterable;
    private final VariantStorageMetadataManager metadataManager;
    private Logger logger = LoggerFactory.getLogger(ChromDensityVariantAggregationExecutor.class);

    public ChromDensityVariantAggregationExecutor(VariantIterable iterable, VariantStorageMetadataManager metadataManager) {
        this.iterable = iterable;
        this.metadataManager = metadataManager;
    }

    @Override
    protected boolean canUseThisExecutor(Query query, QueryOptions options, String facet) throws Exception {
        return isPureChromDensityFacet(facet);
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

    @Override
    protected VariantQueryResult<FacetField> aggregation(Query query, QueryOptions options, String facet) throws Exception {
        StopWatch stopWatch = StopWatch.createStarted();

        String[] split = facet.split(NESTED_FACET_SEPARATOR);
        String chromDensityFacet = split[0];
        FieldVariantAccumulator<Variant> nestedFieldAccumulator;
        if (split.length == 2) {
            String nestedFieldName;
            nestedFieldName = split[1];
            if (!ACCEPTED_CHROM_DENSITY_NESTED.contains(nestedFieldName)) {
                throw invalidNestedField(nestedFieldName);
            }
            switch (nestedFieldName) {
                case "type":
                    nestedFieldAccumulator = new VariantTypeAccumulator<>(Variant::getType);
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
                step = Integer.parseInt(stepStr);
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

            VariantChromDensityAccumulator chromDensityAccumulator =
                    new VariantChromDensityAccumulator(metadataManager, region, nestedFieldAccumulator, step);

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
        return new VariantQueryResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), 1, numMatches, Collections.emptyList(),
                Collections.singletonList(field), null, null);
    }

    private VariantQueryException invalidNestedField(String nestedFieldName) {
        return new VariantQueryException("Unable to calculate " + CHROM_DENSITY + " with nested field " + nestedFieldName);
    }

}
