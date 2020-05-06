package org.opencb.opencga.storage.core.variant.query.executors;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Simplest implementation of the VariantQueryExecutor.
 * Will run the query using directly the {@link VariantDBAdaptor}.
 *
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DBAdaptorVariantQueryExecutor extends VariantQueryExecutor {

    private final VariantDBAdaptor dbAdaptor;
    private Logger logger = LoggerFactory.getLogger(DBAdaptorVariantQueryExecutor.class);
    private static final List<QueryParam> UNSUPPORTED_PARAMS = Arrays.asList(
            VariantQueryUtils.SAMPLE_DE_NOVO,
            VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS,
            VariantQueryUtils.SAMPLE_MENDELIAN_ERROR,
            VariantQueryParam.ANNOT_TRAIT);

    public DBAdaptorVariantQueryExecutor(VariantDBAdaptor dbAdaptor, String storageEngineId, ObjectMap options) {
        super(dbAdaptor.getMetadataManager(), storageEngineId, options);
        this.dbAdaptor = dbAdaptor;
    }

    @Override
    protected Object getOrIterator(Query query, QueryOptions options, boolean iterator) {
        if (iterator) {
            return dbAdaptor.iterator(query, options);
        } else {
            VariantQueryResult<Variant> result = dbAdaptor.get(query, options);
            if (result.getSource() == null || result.getSource().isEmpty()) {
                result.setSource(storageEngineId);
            }
            return result;
        }
    }

    @Override
    public DataResult<Long> count(Query query) {
        return dbAdaptor.count(query);
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) {
        for (QueryParam unsupportedParam : UNSUPPORTED_PARAMS) {
            if (VariantQueryUtils.isValidParam(query, unsupportedParam)) {
                logger.warn("Unsupported variant query param {} in {}",
                        unsupportedParam.key(),
                        DBAdaptorVariantQueryExecutor.class.getSimpleName());
                return false;
            }
        }
        return true;
    }
}
