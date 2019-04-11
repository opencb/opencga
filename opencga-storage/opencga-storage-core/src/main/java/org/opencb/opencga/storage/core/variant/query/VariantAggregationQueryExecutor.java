package org.opencb.opencga.storage.core.variant.query;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser.CHROM_DENSITY;

/**
 * Created on 10/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAggregationQueryExecutor {

    private final VariantSearchManager searchManager;
    private final String dbName;
    private final VariantIterable iterable;
    private final VariantStorageMetadataManager metadataManager;
    private Logger logger = LoggerFactory.getLogger(VariantAggregationQueryExecutor.class);
    public static final Pattern CHROM_DENSITY_PATTERN = Pattern.compile("^" + CHROM_DENSITY + "\\[([a-zA-Z0-9:\\-,*]+)](:(\\d+))?$");

    public VariantAggregationQueryExecutor(VariantSearchManager searchManager, String dbName,
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
    public FacetQueryResult facet(Query query, QueryOptions options) {
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

    protected FacetQueryResult chromDensityAggregation(Query query, QueryOptions options) {
        StopWatch stopWatch = StopWatch.createStarted();
        String facet = options.getString(QueryOptions.FACET);

        int step;
        List<Region> regions = new LinkedList<>();
        Matcher matcher = CHROM_DENSITY_PATTERN.matcher(facet);
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
            throw new VariantQueryException("Malformed aggregation stats query: " + facet);
        }

        if (regions.isEmpty()) {
            throw new VariantQueryException("Unable to calculate aggregated stats query without a region or gene");
        }

        List<FacetQueryResult.Bucket> regionBuckets = new ArrayList<>(regions.size());
        long numMatches = 0;
        for (Region region : regions) {
            Query regionQuery = new Query(query).append(VariantQueryParam.REGION.key(), region);
            VariantDBIterator iterator = iterable.iterator(
                    regionQuery,
                    new QueryOptions()
                            .append(QueryOptions.INCLUDE, VariantField.ID)
                            .append(QueryOptions.SORT, true));

            logger.info("Query : " + regionQuery.toJson());

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
            int numSteps = regionLength / step + 1;

            int[] values = new int[numSteps];
            while (iterator.hasNext()) {
                numMatches++;
                Variant v = iterator.next();
                int idx = (v.getStart() - region.getStart()) / step;
                if (idx < values.length) {
                    values[idx]++;
                }
            }

            regionBuckets.add(buildBucket(region, step, values));
        }
        FacetQueryResult.Field field = new FacetQueryResult.Field(
                CHROM_DENSITY,
                regionBuckets.size(),
                regionBuckets);
        return new FacetQueryResult("", ((int) stopWatch.getTime(TimeUnit.MILLISECONDS)), numMatches, Collections.emptyList(), null,
                Collections.singletonList(field),
                facet
//                query.toJson()
        );
    }

    protected boolean isPureChromDensityFacet(String facet) {
        return facet != null
                && facet.startsWith(CHROM_DENSITY)
                && !facet.contains(FacetQueryParser.FACET_SEPARATOR)
                && !facet.contains(">>");
    }

    private FacetQueryResult.Bucket buildBucket(Region region, int step, int[] values) {
        List<FacetQueryResult.Bucket> valueBuckets = new ArrayList<>(values.length);
        int count = 0;
        for (int i = 0; i < values.length; i++) {
            int value = values[i];
            valueBuckets.add(new FacetQueryResult.Bucket(String.valueOf(i * step + region.getStart()), value, null));
            count += value;
        }

        FacetQueryResult.Field regionField = new FacetQueryResult.Field(VariantField.START.fieldName(), count, valueBuckets)
                .setStart(region.getStart())
                .setEnd(region.getEnd())
                .setStep(step);

        return new FacetQueryResult.Bucket(region.getChromosome(), count, Collections.singletonList(regionField));
    }

}
