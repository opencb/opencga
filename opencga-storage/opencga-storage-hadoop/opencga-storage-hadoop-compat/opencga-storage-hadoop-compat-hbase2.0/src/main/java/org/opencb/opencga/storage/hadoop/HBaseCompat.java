package org.opencb.opencga.storage.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.SnappyCodec;
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
        return false;
    }

    @Override
    public PhoenixCompatApi getPhoenixCompat() {
        return new PhoenixCompat();
    }

    @Override
    public List<ServerName> getServerList(Admin admin) throws IOException {
        return new ArrayList<>(admin.getClusterStatus().getServers());
    }

    public byte[][] getTableStartKeys(Admin admin, Table table) throws IOException {
        List<RegionInfo> regions = admin.getRegions(table.getName());
        regions.sort((o1, o2) -> Bytes.compareTo(o1.getStartKey(), o2.getStartKey()));
        byte[][] startKeys = new byte[regions.size()][];
        for (int i = 0; i < regions.size(); i++) {
            startKeys[i] = regions.get(i).getStartKey();
        }
        return startKeys;
    }

    @Override
    public boolean isSnappyAvailable() {
        return SnappyCodec.isNativeCodeLoaded();
    }
}
