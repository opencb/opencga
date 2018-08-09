package org.opencb.opencga.catalog.stats.solr.converters;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.common.util.NamedList;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.FacetedQueryResultItem;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by wasim on 09/07/18.
 */
public class SolrFacetUtil {

    protected static Logger logger = LoggerFactory.getLogger(CatalogSolrQueryParser.class);

    public static FacetedQueryResultItem toFacetedQueryResultItem(QueryOptions queryOptions, QueryResponse response) {

        String countName;

        // process Solr facet fields
        List<FacetedQueryResultItem.Field> fields = new ArrayList<>();
        if (response.getFacetFields() != null) {
            for (FacetField solrField : response.getFacetFields()) {
                FacetedQueryResultItem.Field field = new FacetedQueryResultItem().new Field();
                field.setName(solrField.getName());

                long total = 0;
                List<FacetedQueryResultItem.Count> counts = new ArrayList<>();
                for (FacetField.Count solrCount : solrField.getValues()) {
                    countName = solrCount.getName();

                    FacetedQueryResultItem.Count count = new FacetedQueryResultItem()
                            .new Count(countName, solrCount.getCount(), null);
                    counts.add(count);
                    total += solrCount.getCount();
                }
                // initialize field
                field.setTotal(total);
                field.setCounts(counts);

                fields.add(field);
            }
        }

        // process Solr facet pivots
        if (response.getFacetPivot() != null) {
            NamedList<List<PivotField>> facetPivot = response.getFacetPivot();
            for (int i = 0; i < facetPivot.size(); i++) {
                List<PivotField> solrPivots = facetPivot.getVal(i);
                if (solrPivots != null && CollectionUtils.isNotEmpty(solrPivots)) {
                    // init field
                    FacetedQueryResultItem.Field field = new FacetedQueryResultItem().new Field();
                    field.setName(facetPivot.getName(i).split(",")[0]);

                    long total = 0;
                    List<FacetedQueryResultItem.Count> counts = new ArrayList<>();
                    for (PivotField solrPivot : solrPivots) {
                        FacetedQueryResultItem.Field nestedField = processSolrPivot(facetPivot.getName(i), 1, solrPivot);

                        FacetedQueryResultItem.Count count = new FacetedQueryResultItem()
                                .new Count(solrPivot.getValue().toString(),
                                solrPivot.getCount(), nestedField);
                        counts.add(count);
                        total += solrPivot.getCount();
                    }
                    // update field
                    field.setTotal(total);
                    field.setCounts(counts);

                    fields.add(field);
                }
            }
        }

        // process Solr facet range
        List<FacetedQueryResultItem.Range> ranges = new ArrayList<>();
        if (response.getFacetRanges() != null) {
            for (RangeFacet solrRange : response.getFacetRanges()) {
                List<Long> counts = new ArrayList<>();
                long total = 0;
                for (Object objCount : solrRange.getCounts()) {
                    long count = ((RangeFacet.Count) objCount).getCount();
                    total += count;
                    counts.add(count);
                }
                ranges.add(new FacetedQueryResultItem().new Range(solrRange.getName(),
                        (Number) solrRange.getStart(), (Number) solrRange.getEnd(),
                        (Number) solrRange.getGap(), total, counts));
            }
        }

        // process Solr facet intersections
        List<FacetedQueryResultItem.Intersection> intersections = new ArrayList<>();
        Map<String, List<List<String>>> intersectionMap = getInputIntersections(queryOptions);
        if (intersectionMap.size() > 0) {
            if (response.getFacetQuery() != null && response.getFacetQuery().size() > 0) {
                for (String key : intersectionMap.keySet()) {
                    List<List<String>> intersectionLists = intersectionMap.get(key);
                    for (List<String> list : intersectionLists) {
                        FacetedQueryResultItem.Intersection intersection = new FacetedQueryResultItem().new Intersection();
                        intersection.setName(key);
                        intersection.setSize(list.size());
                        if (list.size() == 2) {
                            Map<String, Long> counts = new LinkedHashMap<>();
                            String name = list.get(0);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(1);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(0) + "__" + list.get(1);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            intersection.setCounts(counts);

                            // add to the list
                            intersections.add(intersection);
                        } else if (list.size() == 3) {
                            Map<String, Long> map = new LinkedHashMap<>();
                            Map<String, Long> counts = new LinkedHashMap<>();
                            String name = list.get(0);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(1);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(2);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(0) + "__" + list.get(1);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(0) + "__" + list.get(2);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(1) + "__" + list.get(2);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(0) + "__" + list.get(1) + "__" + list.get(2);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            intersection.setCounts(counts);

                            // add to the list
                            intersections.add(intersection);
                        } else {
                            logger.warn("Facet intersection '" + intersection + "' malformed. The expected intersection format"
                                    + " is 'name:value1:value2[:value3]', value3 is optional");
                        }
                    }
                }
            } else {
                logger.warn("Something wrong happened (intersection input and output mismatch).");
            }
        }
        return new FacetedQueryResultItem(fields, ranges, intersections);
    }


    private static FacetedQueryResultItem.Field processSolrPivot(String name, int index, PivotField pivot) {
        String countName;
        FacetedQueryResultItem.Field field = null;
        if (pivot.getPivot() != null && CollectionUtils.isNotEmpty(pivot.getPivot())) {
            field = new FacetedQueryResultItem().new Field();
            field.setName(name.split(",")[index]);

            long total = 0;
            List<FacetedQueryResultItem.Count> counts = new ArrayList<>();
            for (PivotField solrPivot : pivot.getPivot()) {
                FacetedQueryResultItem.Field nestedField = processSolrPivot(name, index + 1, solrPivot);

                countName = solrPivot.getValue().toString();

                FacetedQueryResultItem.Count count = new FacetedQueryResultItem()
                        .new Count(countName, solrPivot.getCount(), nestedField);

                counts.add(count);
                total += solrPivot.getCount();

            }
            field.setTotal(total);
            field.setCounts(counts);
        }

        return field;
    }

    private static Map<String, List<List<String>>> getInputIntersections(QueryOptions queryOptions) {
        Map<String, List<List<String>>> inputIntersections = new HashMap<>();
        if (queryOptions.containsKey(QueryOptions.FACET)
                && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            String[] intersections = queryOptions.getString(QueryOptions.FACET).split("[;]");

            for (String intersection : intersections) {
                String[] splitA = intersection.split(":");
                if (splitA.length == 2) {
                    String[] splitB = splitA[1].split("\\^");
                    if (splitB.length == 2 || splitB.length == 3) {
                        if (!inputIntersections.containsKey(splitA[0])) {
                            inputIntersections.put(splitA[0], new LinkedList<>());
                        }
                        List<String> values = new LinkedList<>();
                        for (int i = 0; i < splitB.length; i++) {
                            values.add(splitB[i]);
                        }
                        inputIntersections.get(splitA[0]).add(values);
                    }
                }
            }
        }
        return inputIntersections;
    }

}
