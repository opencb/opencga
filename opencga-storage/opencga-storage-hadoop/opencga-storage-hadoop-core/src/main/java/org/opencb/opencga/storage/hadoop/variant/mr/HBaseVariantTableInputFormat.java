package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.adaptors.VariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.metadata.local.LocalVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.hadoop.io.HDFSIOConnector;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseVariantConverterConfiguration;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;

import java.io.IOException;
import java.net.URI;
import java.util.function.Function;

import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.*;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantTableInputFormat extends AbstractHBaseVariantTableInputFormat<Variant> {

    protected Function<Result, Variant> initConverter(Configuration configuration) throws IOException {
        VariantQueryProjection projection;


        VariantStorageMetadataDBAdaptorFactory dbAdaptorFactory;
        if (configuration.getBoolean(METADATA_MANAGER_LOCAL, false)) {
            URI[] cacheFiles = DistributedCache.getCacheFiles(configuration);
            dbAdaptorFactory = new LocalVariantStorageMetadataDBAdaptorFactory(cacheFiles, new HDFSIOConnector(configuration));
        } else {
            VariantTableHelper helper = new VariantTableHelper(configuration);
            dbAdaptorFactory = new HBaseVariantStorageMetadataDBAdaptorFactory(helper);
        }

        VariantStorageMetadataManager vsmm = new VariantStorageMetadataManager(dbAdaptorFactory);
        Query query = getQueryFromConfig(configuration);
        QueryOptions queryOptions = getQueryOptionsFromConfig(configuration);
        projection = new VariantQueryProjectionParser(vsmm).parseVariantQueryProjection(query, queryOptions);

        HBaseToVariantConverter<VariantRow> converter = HBaseToVariantConverter.fromVariantRow(vsmm, getScanColumns(configuration))
                .configure(HBaseVariantConverterConfiguration
                        .builder(configuration)
                        .setProjection(projection)
                        .build());
        return from -> converter.convert(new VariantRow(from));
    }

}
