package org.opencb.opencga.storage.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
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
}
