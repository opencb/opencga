package org.opencb.opencga.storage.core.variant.query.executors;

import org.apache.commons.lang3.StringUtils;
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
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.filters.VariantFilterBuilder;

import java.util.*;
import java.util.function.Predicate;

public class BreakendVariantQueryExecutor extends VariantQueryExecutor {

    private final VariantQueryExecutor delegatedQueryExecutor;
    private final VariantDBAdaptor variantDBAdaptor;
    private final VariantFilterBuilder filterBuilder;

    public BreakendVariantQueryExecutor(String storageEngineId, ObjectMap options,
                                        VariantQueryExecutor delegatedQueryExecutor, VariantDBAdaptor variantDBAdaptor) {
        super(variantDBAdaptor.getMetadataManager(), storageEngineId, options);
        this.delegatedQueryExecutor = delegatedQueryExecutor;
        this.variantDBAdaptor = variantDBAdaptor;
        filterBuilder = new VariantFilterBuilder();
    }

    @Override
    public boolean canUseThisExecutor(ParsedVariantQuery variantQuery) throws StorageEngineException {
        VariantQuery query = variantQuery.getQuery();
        return query.getString(VariantQueryParam.TYPE.key()).equals(VariantType.BREAKEND.name())
                && VariantQueryUtils.isValidParam(query, VariantQueryParam.GENOTYPE);
    }

    @Override
    protected Object getOrIterator(ParsedVariantQuery variantQuery, boolean getIterator) throws StorageEngineException {
        int limit = variantQuery.getLimitOr(-1);
        int skip = variantQuery.getSkip();
        boolean count = variantQuery.getCount() && !getIterator;
        int approximateCountSamplingSize = variantQuery.getApproximateCountSamplingSize();
        Query baseQuery = baseQuery(variantQuery.getQuery());
        Predicate<Variant> variantLocalFilter = filterBuilder.buildFilter(variantQuery);

        // Copy to avoid modifications to input query
        ParsedVariantQuery delegatedVariantQuery = new ParsedVariantQuery(variantQuery);
        QueryOptions options = new QueryOptions(variantQuery.getInputOptions());
        options.remove(QueryOptions.SKIP);
        delegatedVariantQuery.setSkip(0);
        if (limit >= 0) {
            int tmpLimit = skip + limit * 4;
            if (count && tmpLimit < approximateCountSamplingSize) {
                tmpLimit = approximateCountSamplingSize;
            }
            options.put(QueryOptions.LIMIT, tmpLimit);
            delegatedVariantQuery.setLimit(tmpLimit);
        }
        delegatedVariantQuery.setInputOptions(options);

        if (getIterator) {
            VariantDBIterator iterator = delegatedQueryExecutor.iterator(delegatedVariantQuery);
            iterator = iterator.mapBuffered(l -> getBreakendPairs(0, baseQuery, variantLocalFilter, l), 100);
            iterator = iterator.localLimitSkip(limit, skip);
            return iterator;
        } else {
            VariantQueryResult<Variant> queryResult = delegatedQueryExecutor.get(delegatedVariantQuery);
            List<Variant> results = queryResult.getResults();
            results = getBreakendPairs(0, baseQuery, variantLocalFilter, results);
            if (queryResult.getNumMatches() < delegatedVariantQuery.getLimitOr(-1)) {
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
        List<Region> regions = new ArrayList<>(variants.size());
        for (Variant variant : variants) {
            BreakendMate mate = variant.getSv().getBreakend().getMate();
            String homlen = variant.getStudies().get(0).getFiles().get(0).getData().get("HOMLEN");
            int buffer = 50; // get from configuration
            if (homlen != null) {
                if (StringUtils.isNumeric(homlen)) {
                    buffer = 10 + Integer.parseInt(homlen);
                }
            }
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

    private boolean addPair(Predicate<Variant> filter, List<Variant> variantPairs, Variant variant, Variant mateVariant) {
        // Check for duplicated pairs
        if (VariantDBIterator.VARIANT_COMPARATOR.compare(variant, mateVariant) > 0) {
            // The mate variant is "before" the main variant
            // This pair might be discarded if the mate matches the given query
            if (!filter.test(mateVariant)) {
                // Otherwise, both variants are added to the list of variant pairs.
                // But first the "mate" to respect order
                variantPairs.add(mateVariant);
                variantPairs.add(variant);
                return true;
            } else {
                return false;
            }
        } else {
            variantPairs.add(variant);
            variantPairs.add(mateVariant);
            return true;
        }
    }

    private boolean validPair(Predicate<Variant> filter, Variant variant, Variant mateVariant) {
        if (VariantDBIterator.VARIANT_COMPARATOR.compare(variant, mateVariant) > 0) {
            return !filter.test(mateVariant);
        }
        return true;
    }

}
