package org.opencb.opencga.storage.core.variant.query.executors;

import com.google.common.collect.Iterators;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.BreakendMate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.filters.VariantFilterBuilder;

import java.util.*;
import java.util.function.Predicate;

public class BreakendVariantQueryExecutor extends VariantQueryExecutor {

    private final VariantQueryExecutor delegatedQueryExecutor;
    private final VariantDBAdaptor variantDBAdaptor;
    private final VariantFilterBuilder filterBuilder;

    public BreakendVariantQueryExecutor(VariantStorageMetadataManager metadataManager, String storageEngineId, ObjectMap options,
                                        VariantQueryExecutor delegatedQueryExecutor, VariantDBAdaptor variantDBAdaptor) {
        super(metadataManager, storageEngineId, options);
        this.delegatedQueryExecutor = delegatedQueryExecutor;
        this.variantDBAdaptor = variantDBAdaptor;
        filterBuilder = new VariantFilterBuilder(metadataManager);
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) throws StorageEngineException {
        return query.getString(VariantQueryParam.TYPE.key()).equals(VariantType.BREAKEND.name())
                && VariantQueryUtils.isValidParam(query, VariantQueryParam.GENOTYPE);
    }

    @Override
    protected Object getOrIterator(Query query, QueryOptions options, boolean getIterator) throws StorageEngineException {
        int limit = options.getInt(QueryOptions.LIMIT);
        int skip = options.getInt(QueryOptions.SKIP);
        boolean count = options.getBoolean(QueryOptions.COUNT);
        int approximateCountSamplingSize = options.getInt(
                VariantStorageOptions.APPROXIMATE_COUNT_SAMPLING_SIZE.key(),
                VariantStorageOptions.APPROXIMATE_COUNT_SAMPLING_SIZE.defaultValue());
        Query baseQuery = baseQuery(query);
        Predicate<Variant> variantLocalFilter = filterBuilder.buildFilter(query, options);


        if (getIterator) {
            VariantDBIterator iterator = delegatedQueryExecutor.iterator(query, options);
            iterator = iterator.mapBuffered(l -> getBreakendPairs(0, baseQuery, variantLocalFilter, l), 100);
            Iterators.advance(iterator, skip);
            iterator = iterator.localSkip(skip);
            if (limit > 0) {
                iterator = iterator.localLimit(limit);
            }
            return iterator;
        } else {
            // Copy to avoid modifications to input options
            options = new QueryOptions(options);
            options.remove(QueryOptions.SKIP);
            int tmpLimit = skip + limit * 2;
            if (count && tmpLimit < approximateCountSamplingSize) {
                tmpLimit = approximateCountSamplingSize;
            }
            options.put(QueryOptions.LIMIT, tmpLimit);
            VariantQueryResult<Variant> queryResult = delegatedQueryExecutor.get(query, options);
            List<Variant> results = queryResult.getResults();
            results = getBreakendPairs(0, baseQuery, variantLocalFilter, results);
            if (queryResult.getNumMatches() < tmpLimit) {
                // Exact count!!
                queryResult.setApproximateCount(false);
                queryResult.setNumMatches(results.size());
            } else if (queryResult.getNumMatches() > 0) {
                // Approx count. Just ignore duplicated pairs
                queryResult.setApproximateCount(true);
                queryResult.setNumMatches(queryResult.getNumMatches() * 2);
            }
            if (skip > results.size()) {
                results = Collections.emptyList();
            } else if (skip > 0) {
                results = results.subList(skip, results.size());
            }
            if (results.size() > limit) {
                results = results.subList(0, limit);
            }
            queryResult.setResults(results);
            queryResult.setNumResults(results.size());

            return queryResult;
        }
    }


    protected VariantDBIterator iterator(Query query, QueryOptions options, int batchSize) throws StorageEngineException {

        int limit = options.getInt(QueryOptions.LIMIT);
        int skip = options.getInt(QueryOptions.SKIP);
        Query baseQuery = baseQuery(query);
        Predicate<Variant> variantLocalFilter = filterBuilder.buildFilter(query, options);

        VariantDBIterator iterator = delegatedQueryExecutor.iterator(query, options);
        iterator = iterator.mapBuffered(l -> getBreakendPairs(0, baseQuery, variantLocalFilter, l), batchSize);
        Iterators.advance(iterator, skip);
        iterator = iterator.localSkip(skip);
        if (limit > 0) {
            iterator = iterator.localLimit(limit);
        }
        return iterator;
    }

    private Query baseQuery(Query query) {
        return subQuery(query,
                VariantQueryParam.STUDY,
                VariantQueryParam.TYPE,
                VariantQueryParam.INCLUDE_SAMPLE,
                VariantQueryParam.INCLUDE_SAMPLE_ID,
                VariantQueryParam.INCLUDE_GENOTYPE,
                VariantQueryParam.INCLUDE_FILE
        );
    }

    private Query subQuery(Query query, QueryParam... queryParams) {
        Query subQuery = new Query();
        for (QueryParam queryParam : queryParams) {
            subQuery.putIfNotNull(queryParam.key(), query.get(queryParam.key()));
        }
        return subQuery;
    }

    private List<Variant> getBreakendPairs(int samplePosition, Query baseQuery, Predicate<Variant> filter, List<Variant> variants) {
        if (variants.isEmpty()) {
            return variants;
        }
        // Copy query to avoid propagating modifications
        baseQuery = new Query(baseQuery);
//        System.out.println("variants = " + variants);
        List<Region> regions = new ArrayList<>(variants.size());
        for (Variant variant : variants) {
            BreakendMate mate = variant.getSv().getBreakend().getMate();
            int buffer = 50;
            regions.add(new Region(mate.getChromosome(), Math.max(1, mate.getPosition() - buffer), mate.getPosition() + buffer));
        }
        baseQuery.put(VariantQueryParam.REGION.key(), regions);

        Map<String, Variant> mateVariantsMap = new HashMap<>(variants.size());
        for (Variant mateVariant : variantDBAdaptor.get(baseQuery, new QueryOptions()).getResults()) {
            if (mateVariant.getStudies().isEmpty()) {
                // Under weird situations, we might get empty studies list. Just discard them.
                continue;
            }
            StudyEntry mateStudyEntry = mateVariant.getStudies().get(0);
            SampleEntry sampleEntry = mateStudyEntry.getSample(samplePosition);
            if (sampleEntry == null || sampleEntry.getFileIndex() == null) {
                // Discard missing samples, or samples without file
                // This might happen because we are not filtering by sample when getting the candidate mate-variants.
                continue;
            }
            String vcfId = mateStudyEntry.getFile(sampleEntry.getFileIndex()).getData().get(StudyEntry.VCF_ID);
            mateVariantsMap.put(vcfId, mateVariant);
        }

        List<Variant> variantPairs = new ArrayList<>(variants.size() * 2);
        for (Variant variant : variants) {
            StudyEntry studyEntry = variant.getStudies().get(0);
            FileEntry file = studyEntry.getFile(studyEntry.getSample(samplePosition).getFileIndex());
            String mateid = file.getData().get("MATEID");
            Variant mateVariant = mateVariantsMap.get(mateid);
            if (mateVariant == null) {
                throw new VariantQueryException("Unable to find mate of variant " + variant + " with MATEID=" + mateid);
            }

            addPair(filter, variantPairs, variant, mateVariant);
        }
        return variantPairs;
    }

    private void addPair(Predicate<Variant> filter, List<Variant> variantPairs, Variant variant, Variant mateVariant) {
        // Check for duplicated pairs
        if (VariantDBIterator.VARIANT_COMPARATOR.compare(variant, mateVariant) > 0) {
            // The mate variant is "before" the main variant
            // This pair might be discarded if the mate matches the given query
            if (!filter.test(mateVariant)) {
                // Otherwise, both variants are added to the list of variant pairs.
                // But first the "mate" to respect order
                variantPairs.add(mateVariant);
                variantPairs.add(variant);
            }
        } else {
            variantPairs.add(variant);
            variantPairs.add(mateVariant);
        }
    }

    private boolean validPair(Predicate<Variant> filter, Variant variant, Variant mateVariant) {
        if (VariantDBIterator.VARIANT_COMPARATOR.compare(variant, mateVariant) > 0) {
            return !filter.test(mateVariant);
        }
        return true;
    }

}
