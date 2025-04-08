package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseVariantConverterConfiguration;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryFromConfig;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryOptionsFromConfig;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PhoenixVariantTableInputFormat
        extends TransformInputFormat<NullWritable, PhoenixVariantTableInputFormat.VariantDBWritable, Variant> {

    @Override
    protected void init(Configuration configuration) throws IOException {
        // Ensure PhoenixDriver is registered
        if (PhoenixDriver.INSTANCE == null) {
            throw new IOException("Error registering PhoenixDriver");
        }
        PhoenixConfigurationUtil.setInputClass(configuration, VariantDBWritable.class);
        inputFormat = new CustomPhoenixInputFormat<>();
    }

    @Override
    public RecordReader<NullWritable, Variant> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (inputFormat == null) {
            init(context.getConfiguration());
        }
        RecordReader<NullWritable, VariantDBWritable> recordReader = inputFormat.createRecordReader(split, context);

        return new RecordReaderTransform<>(recordReader, VariantDBWritable::getVariant);
    }

    public static class VariantDBWritable implements DBWritable, Configurable, Closeable {
        private Configuration conf;
        private HBaseToVariantConverter<ResultSet> converter;
        private Variant variant;
        private VariantStorageMetadataManager metadataManager;

        @Override
        public void write(PreparedStatement statement) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            variant = converter.convert(resultSet);
        }

        public Variant getVariant() {
            return variant;
        }

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            initConverter(conf);
        }

        private void initConverter(Configuration conf) {
            VariantTableHelper helper = new VariantTableHelper(conf);
            HBaseVariantStorageMetadataDBAdaptorFactory dbAdaptorFactory = new HBaseVariantStorageMetadataDBAdaptorFactory(helper);
            Query query = getQueryFromConfig(conf);
            QueryOptions queryOptions = getQueryOptionsFromConfig(conf);
            metadataManager = new VariantStorageMetadataManager(dbAdaptorFactory);
            VariantQueryProjection projection = new VariantQueryProjectionParser(metadataManager)
                    .parseVariantQueryProjection(query, queryOptions);


            converter = HBaseToVariantConverter.fromResultSet(metadataManager)
                    .configure(HBaseVariantConverterConfiguration.builder(conf)
                            .setProjection(projection)
                            .build());
        }

        @Override
        public Configuration getConf() {
            return conf;
        }

        @Override
        public void close() throws IOException {
            metadataManager.close();
        }
    }

}
