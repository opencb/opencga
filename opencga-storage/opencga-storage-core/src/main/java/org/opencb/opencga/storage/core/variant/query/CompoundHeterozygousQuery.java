package org.opencb.opencga.storage.core.variant.query;

import com.google.common.collect.Iterators;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

/**
 * Created on 05/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CompoundHeterozygousQuery {

    public static final String HET = "0/1,0|1,1|0";
    public static final String REF = "0/0,0|0,0";
    public static final String MISSING_SAMPLE = "-";
    private final VariantIterable iterable;
    private static Logger logger = LoggerFactory.getLogger(CompoundHeterozygousQuery.class);

    protected static final Comparator<Variant> VARIANT_COMPARATOR = Comparator.comparing(Variant::getChromosome)
            .thenComparing(Variant::getStart)
            .thenComparing(Variant::getEnd)
            .thenComparing(Variant::getReference)
            .thenComparing(Variant::getAlternate)
            .thenComparing(Variant::toString);


    public CompoundHeterozygousQuery(VariantIterable iterable) {
        this.iterable = iterable;
    }

    public VariantQueryResult<Variant> get(String study, String proband, String father, String mother, Query query, QueryOptions options) {
        return (VariantQueryResult<Variant>) getOrIterator(study, proband, father, mother, query, options, false);
    }

    public VariantDBIterator iterator(String study, String proband, String father, String mother, Query query, QueryOptions options) {
        return (VariantDBIterator) getOrIterator(study, proband, father, mother, query, options, true);
    }

    private Object getOrIterator(String study, String proband, String father, String mother, Query query, QueryOptions options,
                                 boolean iterator) {

        if (father.equals(MISSING_SAMPLE) && mother.equals(MISSING_SAMPLE)) {
            throw new VariantQueryException("Require at least one parent to get compound heterozygous");
        }

        // Prepare query and options
        int skip = Math.max(0, options.getInt(QueryOptions.SKIP));
        int limit = Math.max(0, options.getInt(QueryOptions.LIMIT));
        options = buildQueryOptions(options);

        query = new Query(query);
        List<String> includeSample = getAndCheckIncludeSample(query, proband, father, mother);

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), VariantQueryUtils.LOF_EXTENDED_SET)
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), VariantAnnotationUtils.PROTEIN_CODING)
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSample);

        // Execute query
        Iterator<Variant> rawIterator = getRawIterator(proband, father, mother, query, options);

        // Filter compound heterozygous
        Map<String, List<Variant>> compoundHeterozygous = ModeOfInheritance.compoundHeterozygous(rawIterator,
                includeSample.indexOf(proband),
                includeSample.indexOf(mother),
                includeSample.indexOf(father));

        // Flat map
        TreeSet<Variant> treeSet = new TreeSet<>(VARIANT_COMPARATOR);
        for (List<Variant> variants : compoundHeterozygous.values()) {
            treeSet.addAll(variants);
        }
        logger.debug("Got " + compoundHeterozygous.size() + " compHet groups with "
                + compoundHeterozygous.values().stream().mapToInt(List::size).sum() + " variants, "
                + "of which " + treeSet.size() + " are unique variants");

        // Skip - limit
        Stream<Variant> stream = treeSet.stream();
        if (skip > 0) {
            stream = stream.skip(skip);
        }
        if (limit > 0) {
            stream = stream.limit(limit);
        }

        // Return either an iterator or a query result
        if (iterator) {
            return VariantDBIterator.wrapper(stream.iterator());
        } else {
            VariantQueryResult<Variant> queryResult = VariantDBIterator.wrapper(stream.iterator())
                    .toQueryResult(Collections.singletonMap(study, includeSample));
            queryResult.setNumTotalResults(treeSet.size());
            return queryResult;
        }
    }

    private QueryOptions buildQueryOptions(QueryOptions options) {
        options = new QueryOptions(options); // copy options
        Set<VariantField> includeFields = VariantField.getIncludeFields(options);
        includeFields.add(VariantField.ANNOTATION);
        includeFields.add(VariantField.ANNOTATION_CONSEQUENCE_TYPES);
        includeFields.add(VariantField.STUDIES_SAMPLES_DATA);

        options.put(QueryOptions.INCLUDE, includeFields.stream().map(VariantField::fieldName).collect(Collectors.joining(",")));
        options.remove(QueryOptions.EXCLUDE);
        options.remove(VariantField.SUMMARY);

        options.remove(QueryOptions.LIMIT);
        options.remove(QueryOptions.SKIP);

        options.put(QueryOptions.SKIP_COUNT, true);
        options.put(VariantStorageEngine.Options.APPROXIMATE_COUNT.key(), false);
        return options;
    }

    private List<String> getAndCheckIncludeSample(Query query, String proband, String father, String mother) {
        List<String> includeSamples;
        if (isValidParam(query, VariantQueryParam.INCLUDE_SAMPLE)
                && !query.getString(VariantQueryParam.INCLUDE_SAMPLE.key()).equals(VariantQueryUtils.ALL)) {
            includeSamples = query.getAsStringList(VariantQueryParam.INCLUDE_SAMPLE.key());

            // Check it has all required members
            if (!includeSamples.contains(proband)) {
                throw VariantQueryException.malformedParam(VariantQueryParam.INCLUDE_SAMPLE, includeSamples.toString(),
                        "Can not compute CompoundHeterozygous not including the proband in the query");
            }
            if (!mother.equals(MISSING_SAMPLE) && !includeSamples.contains(mother)) {
                throw VariantQueryException.malformedParam(VariantQueryParam.INCLUDE_SAMPLE, includeSamples.toString(),
                        "Can not compute CompoundHeterozygous not including the mother in the query");
            }
            if (!father.equals(MISSING_SAMPLE) && !includeSamples.contains(father)) {
                throw VariantQueryException.malformedParam(VariantQueryParam.INCLUDE_SAMPLE, includeSamples.toString(),
                        "Can not compute CompoundHeterozygous not including the father in the query");
            }
        } else {
            if (father.equals(MISSING_SAMPLE)) {
                includeSamples = Arrays.asList(proband, mother);
            } else if (mother.equals(MISSING_SAMPLE)) {
                includeSamples = Arrays.asList(proband, father);
            } else {
                includeSamples = Arrays.asList(proband, father, mother);
            }
        }
        return includeSamples;
    }

    private Iterator<Variant> getRawIterator(String proband, String father, String mother, Query query, QueryOptions options) {
        if (father.equals(MISSING_SAMPLE) || mother.equals(MISSING_SAMPLE)) {
            // Single parent iterator
            String parent = father.equals(MISSING_SAMPLE) ? mother : father;

            query = new Query(query).append(VariantQueryParam.GENOTYPE.key(),
                    proband + IS + HET + AND + parent + IS + REF + OR + HET);

            return iterable.iterator(query, options);
        } else {
            // Multi parent iterator
            Query query1 = new Query(query).append(VariantQueryParam.GENOTYPE.key(),
                    proband + IS + HET + AND + father + IS + HET + AND + mother + IS + REF);
            Query query2 = new Query(query).append(VariantQueryParam.GENOTYPE.key(),
                    proband + IS + HET + AND + father + IS + REF + AND + mother + IS + HET);

            VariantDBIterator iterator1 = iterable.iterator(query1, options);
            VariantDBIterator iterator2 = iterable.iterator(query2, options);

//        UnionMultiVariantKeyIterator unionIterator = new UnionMultiVariantKeyIterator(Arrays.asList(iterator1, iterator2));
            return Iterators.concat(iterator1, iterator2);
        }
    }


}
