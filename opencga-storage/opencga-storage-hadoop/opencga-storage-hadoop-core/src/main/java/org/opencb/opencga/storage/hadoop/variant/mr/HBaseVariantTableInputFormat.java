package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseVariantConverterConfiguration;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;

import java.io.IOException;
import java.util.function.Function;

import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryFromConfig;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryOptionsFromConfig;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantTableInputFormat extends AbstractHBaseVariantTableInputFormat<Variant> {

    protected Function<Result, Variant> initConverter(Configuration configuration) throws IOException {
        VariantTableHelper helper = new VariantTableHelper(configuration);
        VariantQueryProjection projection;

        HBaseVariantStorageMetadataDBAdaptorFactory dbAdaptorFactory = new HBaseVariantStorageMetadataDBAdaptorFactory(helper);
        try (VariantStorageMetadataManager scm = new VariantStorageMetadataManager(dbAdaptorFactory)) {
            Query query = getQueryFromConfig(configuration);
            QueryOptions queryOptions = getQueryOptionsFromConfig(configuration);
            projection = VariantQueryProjectionParser.parseVariantQueryFields(query, queryOptions, scm);
        }

        HBaseToVariantConverter<Result> converter = HBaseToVariantConverter.fromResult(helper)
                .configure(HBaseVariantConverterConfiguration
                        .builder(configuration)
                        .setProjection(projection)
                        .build());
        return converter::convert;
    }

}
