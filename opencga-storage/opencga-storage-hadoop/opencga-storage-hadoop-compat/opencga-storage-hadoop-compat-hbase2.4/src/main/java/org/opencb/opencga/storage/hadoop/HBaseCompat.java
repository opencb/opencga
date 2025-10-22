package org.opencb.opencga.storage.hadoop;

import com.lmax.disruptor.EventFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.tephra.TransactionSystemClient;
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
        return new Class<?>[]{TransactionSystemClient.class, EventFactory.class};
    }
}
