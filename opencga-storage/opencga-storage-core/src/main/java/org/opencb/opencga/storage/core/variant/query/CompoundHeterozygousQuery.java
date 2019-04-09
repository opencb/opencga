package org.opencb.opencga.storage.core.variant.query;

import com.google.common.collect.Iterators;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.AND;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.IS;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.isValidParam;

/**
 * Created on 05/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CompoundHeterozygousQuery {

    public static final String HET = "0/1,0|1,1|0";
    public static final String REF = "0/0,0|0,0";
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

        query = new Query(query);
        logger.info("query = " + query.toJson());
        options = new QueryOptions(options); // copy options

        logger.info("options = " + options.toJson());

        Set<VariantField> includeFields = VariantField.getIncludeFields(options);

        includeFields.add(VariantField.ANNOTATION);
        includeFields.add(VariantField.ANNOTATION_CONSEQUENCE_TYPES);
        includeFields.add(VariantField.STUDIES_SAMPLES_DATA);
        options.remove(QueryOptions.EXCLUDE);
        options.remove(VariantField.SUMMARY);
        options.put(QueryOptions.INCLUDE, includeFields.stream().map(VariantField::fieldName).collect(Collectors.joining(",")));

        int skip = Math.max(0, options.getInt(QueryOptions.SKIP));
        int limit = Math.max(0, options.getInt(QueryOptions.LIMIT));
        options.remove(QueryOptions.LIMIT);
        options.remove(QueryOptions.SKIP);

        List<String> includeSamples;
        if (isValidParam(query, VariantQueryParam.INCLUDE_SAMPLE)
                && !query.getString(VariantQueryParam.INCLUDE_SAMPLE.key()).equals(VariantQueryUtils.ALL)) {
            includeSamples = query.getAsStringList(VariantQueryParam.INCLUDE_SAMPLE.key());
            if (!includeSamples.contains(proband)) {
                throw VariantQueryException.malformedParam(VariantQueryParam.INCLUDE_SAMPLE, includeSamples.toString(),
                        "Can not compute CompountHeterozygous not including the proband in the query");
            }
            if (!includeSamples.contains(mother)) {
                throw VariantQueryException.malformedParam(VariantQueryParam.INCLUDE_SAMPLE, includeSamples.toString(),
                        "Can not compute CompountHeterozygous not including the mother in the query");
            }
            if (!includeSamples.contains(father)) {
                throw VariantQueryException.malformedParam(VariantQueryParam.INCLUDE_SAMPLE, includeSamples.toString(),
                        "Can not compute CompountHeterozygous not including the father in the query");
            }
        } else {
            includeSamples = Arrays.asList(proband, father, mother);
        }

        logger.info("optionsFinal = " + options.toJson());
        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), VariantQueryUtils.LOF_EXTENDED_SET)
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), VariantAnnotationUtils.PROTEIN_CODING)
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSamples);
        logger.info("query = " + query.toJson());

        Query query1 = new Query(query).append(VariantQueryParam.GENOTYPE.key(),
                proband + IS + HET + AND + father + IS + HET + AND + mother + IS + REF);
        Query query2 = new Query(query).append(VariantQueryParam.GENOTYPE.key(),
                proband + IS + HET + AND + father + IS + REF + AND + mother + IS + HET);

        logger.info("query1 = " + query1.toJson());
        logger.info("query2 = " + query2.toJson());

        VariantDBIterator iterator1 = iterable.iterator(query1, options);
        VariantDBIterator iterator2 = iterable.iterator(query2, options);

        logger.info("query1 = " + query1.toJson());
        logger.info("query2 = " + query2.toJson());

//        UnionMultiVariantKeyIterator unionIterator = new UnionMultiVariantKeyIterator(Arrays.asList(iterator1, iterator2));
        Iterator<Variant> unionIterator = Iterators.concat(iterator1, iterator2);

        Map<String, List<Variant>> compoundHeterozygous = ModeOfInheritance.compoundHeterozygous(unionIterator,
                includeSamples.indexOf(proband),
                includeSamples.indexOf(mother),
                includeSamples.indexOf(father));


        TreeSet<Variant> treeSet = new TreeSet<>(VARIANT_COMPARATOR);
        for (List<Variant> variants : compoundHeterozygous.values()) {
            treeSet.addAll(variants);
        }
        logger.info("Got " + compoundHeterozygous.size() + " compHet groups with "
                + compoundHeterozygous.values().stream().mapToInt(List::size).sum() + " variants, "
                + "of which " + treeSet.size() + " are unique variants");
        Stream<Variant> stream = treeSet.stream();
        if (skip > 0) {
            stream = stream.skip(skip);
        }
        if (limit > 0) {
            stream = stream.limit(limit);
        }

        if (iterator) {
            return VariantDBIterator.wrapper(stream.iterator());
        } else {
            VariantQueryResult<Variant> queryResult = VariantDBIterator.wrapper(stream.iterator())
                    .toQueryResult(Collections.singletonMap(study, includeSamples));
            queryResult.setNumTotalResults(treeSet.size());
            return queryResult;
        }
    }


}
