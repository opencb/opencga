package org.opencb.opencga.storage.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.PhoenixCompat;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.PhoenixCompatApi;

import java.io.IOException;

public class HBaseCompat implements HBaseCompatApi {
    // singleton
    private static HBaseCompat instance;
    public static HBaseCompat getInstance() {
        if (instance == null) {
            instance = new HBaseCompat();
        }
        return instance;
    }

    @Override
    public void available(Configuration configuration) throws IOException {
        HBaseAdmin.available(configuration);
    }

    @Override
    public PhoenixCompatApi getPhoenixCompat() {
        return new PhoenixCompat();
    }

}
