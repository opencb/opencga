package org.opencb.opencga.storage.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HBaseCompat {

    public static void available(Configuration configuration) throws IOException {
        HBaseAdmin.available(configuration);
    }

}
