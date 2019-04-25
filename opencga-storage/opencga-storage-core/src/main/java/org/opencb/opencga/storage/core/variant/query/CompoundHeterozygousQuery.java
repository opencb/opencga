package org.opencb.opencga.storage.core.variant.query;

import com.google.common.collect.Iterators;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.UnionMultiVariantKeyIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIteratorWithCounts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

/**
 * Created on 05/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CompoundHeterozygousQuery extends AbstractTwoPhasedVariantQueryExecutor {

    public static final String HET = "0/1,0|1,1|0";
    public static final String REF = "0/0,0|0,0";
    public static final String MISSING_SAMPLE = "-";
    private final VariantIterable iterable;
    private static Logger logger = LoggerFactory.getLogger(CompoundHeterozygousQuery.class);

    public CompoundHeterozygousQuery(VariantStorageMetadataManager metadataManager, String storageEngineId, ObjectMap options,
                                     VariantIterable iterable) {
        super(metadataManager, storageEngineId, options, "Unfiltered variant storage");
        this.iterable = iterable;
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) throws StorageEngineException {
        return !options.getBoolean(QueryOptions.COUNT, false) // count is not supported
                && isValidParam(query, VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS);
    }

    @Override
    public QueryResult<Long> count(Query query) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Object getOrIterator(Query query, QueryOptions options, boolean iterator) {
        List<String> samples = query.getAsStringList(VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS.key());
        if (samples.size() != 3) {
            throw VariantQueryException.malformedParam(VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS, String.valueOf(samples));
        }
        return getOrIterator(query.getString(VariantQueryParam.STUDY.key()), samples.get(0), samples.get(1), samples.get(2),
                query, options, iterator);
    }

    @Override
    protected int primaryCount(Query query, QueryOptions options) {
        List<String> samples = query.getAsStringList(VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS.key());
        if (samples.size() != 3) {
            throw VariantQueryException.malformedParam(VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS, String.valueOf(samples));
        }
        return Iterators.size(getRawIterator(samples.get(0), samples.get(1), samples.get(2), query, new QueryOptions()
                .append(QueryOptions.INCLUDE, VariantField.ID.fieldName())));
    }

    public VariantQueryResult<Variant> get(String study, String proband, String father, String mother, Query query, QueryOptions options) {
        return (VariantQueryResult<Variant>) getOrIterator(study, proband, father, mother, query, options, false);
    }

    public VariantDBIterator iterator(String study, String proband, String father, String mother, Query query, QueryOptions options) {
        return (VariantDBIterator) getOrIterator(study, proband, father, mother, query, options, true);
    }

    private Object getOrIterator(String study, String proband, String father, String mother, Query query, QueryOptions inputOptions,
                                 boolean iterator) {
        // Prepare query and options
        int skip = Math.max(0, inputOptions.getInt(QueryOptions.SKIP));
        int limit = Math.max(0, inputOptions.getInt(QueryOptions.LIMIT));
        QueryOptions options = buildQueryOptions(inputOptions);

        query = new Query(query);
        List<String> includeSample = getAndCheckIncludeSample(query, proband, father, mother);

        if (isValidParam(query, VariantQueryParam.ANNOT_BIOTYPE)) {
            String biotype = query.getString(VariantQueryParam.ANNOT_BIOTYPE.key());
            if (!biotype.equals(VariantAnnotationUtils.PROTEIN_CODING)) {
                throw new VariantQueryException("Unsupported " + VariantQueryParam.ANNOT_BIOTYPE.key() + " filter \"" + biotype + "\""
                        + " when filtering by Compound Heterozygous. The only valid value is " + VariantAnnotationUtils.PROTEIN_CODING);
            }
        } else {
            query.append(VariantQueryParam.ANNOT_BIOTYPE.key(), VariantAnnotationUtils.PROTEIN_CODING);
        }

        Set<String> cts = new HashSet<>();
        if (isValidParam(query, VariantQueryParam.ANNOT_CONSEQUENCE_TYPE)) {
            for (String ct : query.getAsStringList(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key())) {
                cts.add(ConsequenceTypeMappings.accessionToTerm.get(VariantQueryUtils.parseConsequenceType(ct)));
            }
//            if (!LOF_EXTENDED_SET.containsAll(cts)) {
//                cts.removeAll(LOF_EXTENDED_SET);
//                throw new VariantQueryException("Unsupported " + VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key() + " filter " + cts
//                        + " when filtering by Compound Heterozygous. Only LOF+Missense accepted");
//            }
        }
//        else {
//            query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), VariantQueryUtils.LOF_EXTENDED_SET);
//        }

        query.append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSample);

        // Execute query
        VariantDBIteratorWithCounts unfilteredIterator
                = new VariantDBIteratorWithCounts(getRawIterator(proband, father, mother, query, options));

        // Filter compound heterozygous
        List<Variant> compoundHeterozygous = ModeOfInheritance.compoundHeterozygous(unfilteredIterator,
                includeSample.indexOf(proband),
                includeSample.indexOf(mother),
                includeSample.indexOf(father),
                limit + skip, cts);

//        logger.debug("Got " + compoundHeterozygous.size() + " compHet groups with "
//                + compoundHeterozygous.values().stream().mapToInt(List::size).sum() + " variants, "
//                + "of which " + treeSet.size() + " are unique variants");

        // Skip
        Iterator<Variant> variantIterator = compoundHeterozygous.iterator();
        Iterators.advance(variantIterator, skip);

        // Return either an iterator or a query result
        if (iterator) {
            return VariantDBIterator.wrapper(variantIterator);
        } else {
            VariantQueryResult<Variant> result = VariantDBIterator.wrapper(variantIterator)
                    .toQueryResult(Collections.singletonMap(study, includeSample));
            setNumTotalResults(unfilteredIterator, result, query, inputOptions);
            return result;
        }
    }

    protected QueryOptions buildQueryOptions(QueryOptions options) {
        options = new QueryOptions(options); // copy options
        Set<VariantField> includeFields = VariantField.getIncludeFields(options);
        includeFields.add(VariantField.ANNOTATION);
        includeFields.add(VariantField.ANNOTATION_CONSEQUENCE_TYPES);
        includeFields.add(VariantField.STUDIES_SAMPLES_DATA);

        VariantField.prune(includeFields);

        options.put(QueryOptions.INCLUDE, includeFields.stream().map(VariantField::fieldName).collect(Collectors.joining(",")));
        options.remove(QueryOptions.EXCLUDE);
        options.remove(VariantField.SUMMARY);

        options.remove(QueryOptions.LIMIT);
        options.remove(QueryOptions.SKIP);

        options.put(QueryOptions.SKIP_COUNT, true);
        options.put(VariantStorageEngine.Options.APPROXIMATE_COUNT.key(), false);
        return options;
    }

    protected List<String> getAndCheckIncludeSample(Query query, String proband, String father, String mother) {
        if (father.equals(MISSING_SAMPLE) && mother.equals(MISSING_SAMPLE)) {
            throw new VariantQueryException("Require at least one parent to get compound heterozygous");
        }

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

    protected VariantDBIterator getRawIterator(String proband, String father, String mother, Query query, QueryOptions options) {
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

            VariantDBIterator iterator1 = iterable.iterator(query1, new QueryOptions(options).append(QueryOptions.SORT, true));
            VariantDBIterator iterator2 = iterable.iterator(query2, new QueryOptions(options).append(QueryOptions.SORT, true));

            return new UnionMultiVariantKeyIterator(Arrays.asList(iterator1, iterator2));
//            return Iterators.concat(iterator1, iterator2);
        }
    }
}
