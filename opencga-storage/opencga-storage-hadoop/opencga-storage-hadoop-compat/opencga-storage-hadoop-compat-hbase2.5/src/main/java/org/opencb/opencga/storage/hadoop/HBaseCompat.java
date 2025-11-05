package org.opencb.opencga.storage.hadoop;

import com.lmax.disruptor.EventFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.PhoenixCompat;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.PhoenixCompatApi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseCompat extends HBaseCompatApi {

    @Override
    public void available(Configuration configuration) throws IOException {
        HBaseAdmin.available(configuration);
    }

    @Override
    public boolean isSolrTestingAvailable() {
        return true;
    }

    @Override
    public PhoenixCompatApi getPhoenixCompat() {
        return new PhoenixCompat();
    }

    @Override
    public List<ServerName> getServerList(Admin admin) throws IOException {
        return new ArrayList<>(admin.getClusterMetrics().getServersName());
    }

    @Override
    public byte[][] getTableStartKeys(Admin admin, Table table) throws IOException {
        return table.getRegionLocator().getStartKeys();
    }

    @Override
    public boolean isSnappyAvailable() {
        // [HADOOP-17125] - Using snappy-java in SnappyCodec - 3.3.1, 3.4.0
        return true;
    }

    @Override
    public Class<?>[] getClassesForDependencyJars() {
        return new Class<?>[]{EventFactory.class};
    }

    @Override
    public void validateConfiguration(Configuration configuration) throws IllegalArgumentException {

        // https://app.clickup.com/t/36631768/TASK-7319
        // https://issues.apache.org/jira/browse/PHOENIX-6761
        // https://issues.apache.org/jira/browse/PHOENIX-6883
        // Ensure MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB is set to, at least, 256MB
        long maxClientMetadataCacheSize = configuration.getLong(QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB,
                QueryServicesOptions.DEFAULT_MAX_CLIENT_METADATA_CACHE_SIZE);
        if (maxClientMetadataCacheSize < (256L * 1024 * 1024)) {
            throw new IllegalArgumentException("Phoenix configuration property '" + QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB
                    + "' is set to " + maxClientMetadataCacheSize + ", which is too low for OpenCGA operations. "
                    + "Please set it to at least 268435456 (256MB).");
        }

        // Must have phoenix ttl disabled
        // OpenCGA doesn't add the "empty cell"
        // Avoid WARN messages "TTLRegionScanner:115 - No empty column cell <table>"
        boolean ttlEnabled = configuration.getBoolean(QueryServices.PHOENIX_TABLE_TTL_ENABLED,
                QueryServicesOptions.DEFAULT_PHOENIX_TABLE_TTL_ENABLED);
        if (ttlEnabled) {
            throw new IllegalArgumentException("Phoenix Table TTL is enabled. "
                    + "This feature is not compatible with OpenCGA. Please disable it by setting '"
                    + QueryServices.PHOENIX_TABLE_TTL_ENABLED + "' to false.");
        }

        int keyValueMaxSize = configuration.getInt("hbase.client.keyvalue.maxsize", -1);
        if (keyValueMaxSize == 0) {
            throw new IllegalArgumentException("HBase configuration property 'hbase.client.keyvalue.maxsize' is set to 0 (unlimited). "
                    + "This should be an accepted value, but Phoenix will read this as 0 bytes, since PHOENIX-6167. "
                    + "Please set it to at least 10 MB.");
        } else if (keyValueMaxSize <= 5 * 1024 * 1024) {
            throw new IllegalArgumentException("HBase configuration property 'hbase.client.keyvalue.maxsize' is set to "
                    + keyValueMaxSize + ". This value is too low for OpenCGA operations. Please set it to at least 10 MB.");
        }
    }
}
