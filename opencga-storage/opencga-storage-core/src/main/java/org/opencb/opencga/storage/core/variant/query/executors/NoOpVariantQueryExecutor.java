package org.opencb.opencga.storage.core.variant.query.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.GENOTYPE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;

/**
 * Look for technically correct queries that will return 0 results.
 */
public class NoOpVariantQueryExecutor extends VariantQueryExecutor {
    public static final String NO_OP = "no-op";
    private static Logger logger = LoggerFactory.getLogger(NoOpVariantQueryExecutor.class);

    public NoOpVariantQueryExecutor(VariantStorageMetadataManager metadataManager, String storageEngineId, ObjectMap options) {
        super(metadataManager, storageEngineId, options);
    }

    @Override
    public boolean canUseThisExecutor(ParsedVariantQuery variantQuery) throws StorageEngineException {
        VariantQuery query = variantQuery.getQuery();
        boolean sampleQuery = false;
        String sample = null;
        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.GENOTYPE)) {
            HashMap<Object, List<String>> gtMap = new HashMap<>();
            VariantQueryUtils.QueryOperation queryOperation = VariantQueryUtils.parseGenotypeFilter(query.getString(GENOTYPE.key()), gtMap);
            if (queryOperation == null || queryOperation == VariantQueryUtils.QueryOperation.AND) {
                for (Map.Entry<Object, List<String>> entry : gtMap.entrySet()) {
                    Object thisSample = entry.getKey();
                    List<String> gts = entry.getValue();
                    if (gts.stream().allMatch(GenotypeClass.MAIN_ALT.predicate())) {
                        sampleQuery = true;
                        sample = thisSample.toString();
                        break;
                    }
                }
            }
        }
        if (sampleQuery) {
            if (checkStatsFilter(query, sample, VariantQueryParam.STATS_ALT)) {
                return true;
            }
            if (checkStatsFilter(query, sample, VariantQueryParam.STATS_MAF)) {
                return true;
            }
        }

        // Check invalid positional filter
        ParsedVariantQuery.VariantQueryXref xrefs = VariantQueryParser.parseXrefs(query);
        boolean withGeneAliasFilter = VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_GENE_ROLE_IN_CANCER)
                || VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_GO)
                || VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_EXPRESSION);
        if (withGeneAliasFilter && xrefs.getGenes().isEmpty()) {
            // We have geneAliasFilter, but it didn't produce any actual gene.
            // If there is no other region or variant filter, we should return no variants.
            if (!VariantQueryUtils.isValidParam(query, REGION) && xrefs.getVariants().isEmpty()) {
                return true;
            }
        }

        if (VariantQueryUtils.NON_EXISTING_REGION.equals(query.getString(REGION.key()))) {
            if (xrefs.getGenes().isEmpty() && xrefs.getVariants().isEmpty()) {
                // Nothing to return
                return true;
            }
        }

        return false;
    }

    /**
     * Check for a VariantStats filter where the filtered freq is below the minimum possible, given the number of samples in the cohort.
     * @param query VariantQuery
     * @param sample A sample in the query that should be in the cohort.
     * @param param The variant stats param.
     * @return If the query would return no values.
     */
    private boolean checkStatsFilter(Query query, String sample, VariantQueryParam param) {
        if (VariantQueryUtils.isValidParam(query, param)) {
            StudyMetadata defaultStudy = VariantQueryParser.getDefaultStudy(query, metadataManager);
            Values<KeyOpValue<String, String>> filter =
                    VariantQueryUtils.parseMultiKeyValueFilterComparators(param, query.getString(param.key()));

            if (filter.getOperation() == null || filter.getOperation() == VariantQueryUtils.QueryOperation.AND) {
                for (KeyOpValue<String, String> keyOp : filter) {
                    String cohort = keyOp.getKey();
                    double value = Double.parseDouble(keyOp.getValue());
                    CohortMetadata cohortMetadata = metadataManager.getCohortMetadata(defaultStudy.getId(), cohort);
                    Integer sampleId = metadataManager.getSampleId(defaultStudy.getId(), sample);
                    if (cohortMetadata.getSamples().contains(sampleId)) {
                        int numSamples = cohortMetadata.getSamples().size();
                        int numAlleles = numSamples * 2;
                        double minFreqPossible = 1.0 / numAlleles;
                        if (keyOp.getOp().equals(VariantQueryUtils.OP_LE) || keyOp.getOp().equals(VariantQueryUtils.OP_LT)) {
                            if (value < minFreqPossible) {
                                logger.info("Filtering by sample '{}', and param '{}' : {}, there is no possible results",
                                        sample,
                                        param.key(),
                                        keyOp);
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    protected Object getOrIterator(ParsedVariantQuery variantQuery, boolean iterator) throws StorageEngineException {
        if (iterator) {
            return VariantDBIterator.emptyIterator();
        } else {
            return new VariantQueryResult<>(0, 0, 0, Collections.emptyList(), Collections.emptyList(), NO_OP, variantQuery);
        }
    }
}
