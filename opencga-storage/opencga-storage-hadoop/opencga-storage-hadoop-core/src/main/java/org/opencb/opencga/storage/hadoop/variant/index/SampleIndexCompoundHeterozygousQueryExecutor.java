package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.collect.Iterators;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.CompoundHeterozygousQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;

import java.util.List;

/**
 * Created by jacobo on 26/04/19.
 */
public class SampleIndexCompoundHeterozygousQueryExecutor extends CompoundHeterozygousQueryExecutor {

    private final SampleIndexDBAdaptor sampleIndexDBAdaptor;

    public SampleIndexCompoundHeterozygousQueryExecutor(
            VariantStorageMetadataManager metadataManager, String storageEngineId, ObjectMap options, VariantIterable iterable,
            SampleIndexDBAdaptor sampleIndexDBAdaptor) {
        super(metadataManager, storageEngineId, options, iterable);
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
    }

    @Override
    protected long primaryCount(Query query, QueryOptions options) {
        // Assume that filter from secondary index is good enough for the approximate count.
        List<String> samples = query.getAsStringList(VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS.key());
        if (samples.size() != 3) {
            throw VariantQueryException.malformedParam(VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS, String.valueOf(samples));
        }
        return Iterators.size(getRawIterator(samples.get(0), samples.get(1), samples.get(2), query, new QueryOptions()
                .append(QueryOptions.INCLUDE, VariantField.ID.fieldName()), sampleIndexDBAdaptor));
    }
}
