package org.opencb.opencga.storage.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.PhoenixCompat;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.PhoenixCompatApi;

import java.io.IOException;

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

}
