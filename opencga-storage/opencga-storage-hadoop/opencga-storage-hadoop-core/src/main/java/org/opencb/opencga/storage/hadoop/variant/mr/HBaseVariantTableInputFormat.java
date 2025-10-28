package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapreduce.JobContext;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.adaptors.VariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseVariantConverterConfiguration;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.metadata.HdfsLocalVariantStorageMetadataDBAdaptorFactory;

import java.io.IOException;
import java.util.function.Function;

import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.*;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantTableInputFormat extends AbstractHBaseVariantTableInputFormat<Variant> {

    protected Function<Result, Variant> initConverter(JobContext context) throws IOException {
        Configuration configuration = context.getConfiguration();
        VariantQueryProjection projection;


        VariantStorageMetadataDBAdaptorFactory dbAdaptorFactory;
        if (configuration.getBoolean(METADATA_MANAGER_LOCAL, false)) {
            dbAdaptorFactory = new HdfsLocalVariantStorageMetadataDBAdaptorFactory(configuration);
        } else {
            dbAdaptorFactory = new HBaseVariantStorageMetadataDBAdaptorFactory(new VariantTableHelper(configuration));
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
